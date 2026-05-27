# Audit: trades.entry_price/exit_price vs Alpaca paper fills

**Window:** 2026-05-14 → 2026-05-21 (7 days)
**Method:** Match (symbol, side, timestamp ±120s) between `trades` rows and Alpaca paper closed orders. Compare `trades.price`/`exit_price` to `filled_avg_price`. Flag |delta| > $0.50.
**Source script:** `/tmp/sr_audit/audit.py`
**Source data:** `/tmp/sr_audit/alpaca_orders.json`, `/tmp/sr_audit/trades.tsv`

---

## Headline blast radius

| Metric | Value |
|---|---|
| Total trades 7d (local DB) | 165 |
| Total Alpaca paper orders 7d | 102 (86 with filled_at) |
| Trades matched to Alpaca fill | 65 / 165 |
| Of matched: **OK (\|delta\| ≤ $0.50)** | 39 (60%) |
| Of matched: **FLAGGED (\|delta\| > $0.50)** | **26 (40%)** |
| Alpaca fills with NO mirroring trade row | 20 / 86 (23%) |
| **PnL distortion (matched sells)** | DB −$136.14 vs broker −$53.90 → **DB overstates losses by $82.24 (153%)** |

**Two distinct integrity issues:**
1. **Price writeback bug** — 40% of Alpaca-routed trades have prices off by >$0.50. Direction is systematic: BUYs recorded too high, SELLs recorded too low. Both biases inflate losses.
2. **Mirror gap** — 23% of Alpaca fills have no corresponding trade row at all.

Additionally, **0 of 165 trade rows** have `alpaca_order_id` populated and **all 165** are marked `execution_type='simulated'`, even though 65+ of them are real Alpaca fills. Provenance metadata is broken.

---

## Top 25 flagged rows (by |delta|)

| id | player | sym | side | local field | DB | Alpaca | delta |
|---|---|---|---|---|---:|---:|---:|
| 2455 | ollie-auto | MDGL | sell | exit_price | 487.57 | 521.53 | **−33.96** |
| 2491 | ollie-auto | COST | sell | exit_price | 1056.81 | 1081.87 | **−25.06** |
| 2463 | neo-matrix | AVGO | buy | price | 429.80 | 410.23 | **+19.57** |
| 2461 | neo-matrix | AVGO | buy | price | 429.80 | 410.33 | **+19.47** |
| 2492 | ollie-auto | PLXS | buy | price | 275.56 | 257.62 | **+17.94** |
| 2436 | ollie-auto | RHP | sell | exit_price | 99.81 | 107.89 | −8.08 |
| 2360 | ollie-auto | SHW | sell | exit_price | 300.59 | 308.38 | −7.79 |
| 2381 | ollie-auto | PTGX | buy | price | 110.29 | 103.33 | +6.96 |
| 2509 | ollie-auto | SR | sell | exit_price | 80.40 | 86.58 | −6.18 |
| 2413 | ollie-auto | JNJ | sell | exit_price | 221.19 | 227.34 | −6.15 |
| 2502 | ollie-auto | SR | buy | price | 92.91 | 86.85 | +6.06 |
| 2351 | neo-matrix | MSFT | buy | price | 415.65 | 410.03 | +5.62 |
| 2470 | ollie-auto | AVB | sell | exit_price | 179.49 | 184.76 | −5.27 |
| 2372 | ollie-auto | CRM | sell | exit_price | 168.94 | 173.25 | −4.31 |
| 2367 | neo-matrix | AVGO | buy | price | 429.80 | 427.31 | +2.49 |
| 2442 | ollie-auto | FTXR | buy | price | 41.31 | 38.87 | +2.44 |
| 2445 | ollie-auto | FTXR | buy | price | 41.31 | 38.87 | +2.44 |
| 2369 | neo-matrix | AVGO | buy | price | 429.80 | 427.39 | +2.41 |
| 2443 | ollie-auto | SNX | buy | price | 236.15 | 233.83 | +2.32 |
| 2446 | ollie-auto | SNX | buy | price | 236.15 | 233.83 | +2.32 |
| 2439 | neo-matrix | AVGO | buy | price | 415.51 | 417.50 | −1.99 |
| 2468 | neo-matrix | AVGO | sell | exit_price | 408.35 | 407.02 | +1.33 |
| 2488 | ollie-auto | PWR | sell | exit_price | 723.59 | 722.38 | +1.21 |
| 2349 | ollie-auto | CRM | buy | price | 166.03 | 166.86 | −0.83 |
| 2346 | ollie-auto | NFLX | buy | price | 88.42 | 87.67 | +0.75 |

## Bias direction

- **SELL rows:** DB exit_price < Alpaca fill in **9 / 11** flagged sells. Magnitude often $5–34. Pattern matches the SR hypothesis — trader is writing limit_price / trigger price (the worst-case the trader sized for), not the actual fill.
- **BUY rows:** DB price > Alpaca fill in **13 / 15** flagged buys. Same pattern in reverse — trader sized against a worst-case BUY limit / pre-trade quote, then got a better fill.

Net effect: trades look strictly worse than they actually were. Both legs inflate losses.

## Repeated-price clue

- AVGO rows 2461 / 2463 both have DB price = $429.80 against Alpaca fills $410.23 / $410.33 — two distinct orders at distinct fills both got the same locked-in $429.80 in the DB. Looks like the trader is recording a snapshot quote from a shared place (signal payload? limit_price field? pre-trade market snapshot?), not the per-order fill returned by Alpaca.
- AVGO 2467 (=$415.51 vs $417.50) breaks the pattern — that one is closer to fill. So whatever source the writer is reading, it sometimes does reflect near-real prices and sometimes doesn't.

## Players with unmatched trades (no Alpaca pair within ±120s)

| player | unmatched | likely cause |
|---|---:|---|
| ollie-auto | 40 | mix of (a) trade closes whose opening BUY was pre-2026-05-14 outside our Alpaca pull; (b) possible >120s drift |
| neo-matrix | 14 | same |
| navigator | 13 | same |
| deepseek-7b-grok4 | 11 | pure-sim player — never routes to Alpaca |
| capitol-trades | 10 | pure-sim |
| ollama-plutus | 7 | pure-sim |
| qwen3-8b-flash | 5 | pure-sim |

33 of the 100 unmatched are likely pure-sim (expected). 67 are real-Alpaca players where the absence is a candidate bug — but the lookback only fetched orders since 2026-05-14, so trades whose open leg is older won't match here.

## Downstream contamination risk

Anything that reads `trades.realized_pnl` or `trades.entry_price` / `exit_price` is downstream of this:

- WR% / PnL dashboards (Game Plan, Leaderboard, glance-row)
- `ollietrades_s6` audit logs
- [[project-hm-deepseek-triage-2026-05-20]] and similar PnL-driven post-mortems
- **Plutus fine-tune corpus** ([[project-hm-plutus-finetuning-v1-scope]]) — Format A grading uses `realized_pnl` as ground truth. **Halt corpus extraction until reconciliation.**
- [[project-hm-ollie-auto-regression]] May-PnL conclusions — the −$145 figure may be partly bookkeeping, not market loss
- [[project-hm-grade-b-fleet-gate-validation]] — Grade-B WR analyses also affected

## What this audit does NOT establish

- Whether the bug is in the BUY-side write, SELL-side write, or both (data shows both, but root cause may be a single shared writer pulling the wrong field).
- Whether the bug is symbol-specific or limit-vs-market-order-specific.
- Whether the bug is recent (last few sprints) or longstanding — only checked 7d window.
- Whether the broker fills are themselves correct (assumed Alpaca API is ground truth; this is reasonable for paper but worth a one-spot Webull cross-check on a single live trade if any exist in window).

## Recommended next actions (NOT executed — audit-only as instructed)

1. **Bank HIGH-priority ticket: `HM-TRADES-PRICE-WRITEBACK-BUG`** with this file as the evidence pack.
2. **Bank HIGH-priority ticket: `HM-TRADES-ALPACA-PROVENANCE`** for the `alpaca_order_id` / `execution_type='simulated'` mis-labelling.
3. **Pause learning-corpus extraction** for Plutus fine-tune until #1 is fixed.
4. Before code changes, identify the writer: `grep -rn 'INSERT INTO trades\|UPDATE trades' engine/` and see which paths write `entry_price` / `exit_price` and what source they read.
5. Fix forward: writer should read `filled_avg_price` from the Alpaca order response, not the trader's internal limit/trigger/snapshot.
6. Backfill: a separate reconciliation script can fix historical rows by re-pulling Alpaca orders and rewriting prices + recomputing realized_pnl. NOT recommended until #1 ships and is stable.
