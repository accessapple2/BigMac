# Orphan Alpaca Positions — 2026-06-12

**HM-STOP-EXECUTION-GAP DEFECT 2.** Alpaca (paper) positions held in the broker
mirror (`alpaca-mirror`) with **no owning active fleet player** — therefore they are
**never stop-loss-checked** (the risk_manager flat stop only runs over auto-tradeable
active players' position rows; `alpaca-mirror` is a passive mirror, excluded).

**Report only — nothing auto-sold** (per directive + RULE #1 / sacred-data).

- alpaca-mirror stock symbols: **29**
- covered by an active player: **7** (BLK, COST, FBTC, MS, PM, UNP, ZS)
- **orphans (unprotected): 22**
- orphan book: cost $5,506 → market $5,280 (-4.1%)

| Symbol | Qty | Entry | Current | P&L % | Mkt Value | Other holders |
|--------|----:|------:|--------:|------:|----------:|---------------|
| AVB | 0.72 | $185.20 | $186.02 | +0.4% | $134 | — |
| AVGO | 0.02 | $413.38 | $380.78 | -7.9% | $8 | — |
| BAP | 0.01 | $345.58 | $372.26 | +7.7% | $4 | — |
| F | 2.01 | $14.69 | $14.79 | +0.7% | $30 | — |
| GM | 1 | $79.73 | $81.37 | +2.1% | $81 | — |
| INTU | 3 | $306.99 | $271.87 | -11.4% | $816 | — |
| IWP | 1.12 | $142.37 | $140.87 | -1.1% | $158 | — |
| KMI | 18 | $33.83 | $31.91 | -5.7% | $574 | — |
| LII | 0.01 | $501.50 | $512.27 | +2.1% | $5 | — |
| LLY | 0.12 | $996.28 | $1146.34 | +15.1% | $138 | — |
| LNTH | 1.19 | $102.50 | $104.37 | +1.8% | $124 | — |
| MDGL | 0.09 | $508.35 | $483.39 | -4.9% | $44 | — |
| MSFT | 0.38 | $417.73 | $387.18 | -7.3% | $147 | — |
| NUGT | 0.01 | $149.02 | $134.47 | -9.8% | $1 | — |
| RBC | 0.1 | $573.22 | $602.50 | +5.1% | $60 | — |
| SPGI | 0.01 | $420.84 | $418.51 | -0.6% | $4 | — |
| SYM | 2.25 | $54.19 | $42.62 | -21.4% | $96 | — |
| TKR | 1.02 | $116.48 | $137.11 | +17.7% | $140 | — |
| WFRD | 0.42 | $112.15 | $101.63 | -9.4% | $43 | — |
| WMB | 35 | $74.01 | $72.16 | -2.5% | $2,526 | — |
| WMG | 1.33 | $34.12 | $28.47 | -16.6% | $38 | — |
| ZM | 1.18 | $105.97 | $93.63 | -11.6% | $110 | — |

## Proposed follow-up (NOT executed)
Adopt orphans under a **guardian player** (e.g. `alpaca-mirror` made auto-tradeable
for exits only, or a dedicated `orphan-guardian`) carrying the **flat 12% guardrail**,
so a stop-loss actually fires on these. Scope as a separate HM with Admiral sign-off
(routing + halt_mode + exit-only-buy-side gate). See HM-STOP-EXECUTION-GAP DEFECT 2.

---

## Adoption + First Sweep — 2026-06-12 (HM-GUARDIAN-ADOPTION, commit da015f1)

**Admiral-approved Option A (entry-based adoption).** Created exit-only player `guardian-of-forever` (may NEVER buy: exit_only + absent from every scanner / AI_SIGNAL_PLAYERS). Adopted all **22 orphans** with avg_entry sourced live from the Alpaca positions API. Guardian routes to **Alpaca Paper** so its flat-12% stops + standard tiered TP **actually close the broker positions**. `alpaca-mirror` rows untouched. Idempotent (re-run: 0 adopted / 22 skipped). Stuck-stop watchdog covers it.

### First sweep executed (5 exits, all routed to Alpaca):

| Symbol | Action | Qty | Entry | Exit | Realized P&L | Alpaca order |
|--------|--------|----:|------:|-----:|-------------:|--------------|
| SYM | Stop-loss −21.3% (full close) | 2.25 | $54.19 | $42.67 | −$25.92 | 98d97c2a |
| WMG | Stop-loss −16.7% (full close) | 1.33 | $34.12 | $28.42 | −$7.58 | 3d692318 |
| LLY | Tiered TP +14.9% (trim) | 0.06 | $996.28 | $1144.23 | +$8.85 | 284c8390 |
| TKR | Tiered TP +17.5% (tier 10%) | 0.51 | $116.48 | $136.81 | +$10.19 | 935ff94e |
| TKR | Tiered TP +17.5% (tier 15%) | 0.255 | $116.48 | $136.81 | +$5.09 | c1377e2b |

Deep bleeders **SYM and WMG closed at the broker** (verified via Alpaca API). Batched NTFY summary delivered to `ollietrades-admin`. Realized P&L logged per position in `trades`.

### Ongoing coverage (acceptance #3)

Guardian now holds **20** positions. `run_guardian_sweep` is scheduled every 10 min (main.py) and runs the same flat-12% stop + tiered TP. Live coverage re-check this cycle:

- **0 further actions** — all 20 remaining positions are within the 12% stop / below TP tiers right now (coverage active, nothing else to fire this cycle).

