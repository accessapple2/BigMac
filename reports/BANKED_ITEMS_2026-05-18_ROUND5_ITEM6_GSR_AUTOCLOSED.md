# HM-METALS-GSR-DATA-WIRE — Auto-closed by Round 3
Date: 2026-05-18
Status: RESOLVED (no fix needed)

## Original symptom (from BANKED_ITEMS_2026-05-18_DASHBOARD_AUDIT.md)
- Bridge Metals panel GSR ratio showed "--" or "No metals data"
- Captain page dump (2026-05-18, prior to firing this MEGA) already
  showed GSR 58.6, SIGNAL: HOLD neutral — looked LIVE

## Round 5 verification (post-firing audit)
```
curl http://127.0.0.1:8080/api/metals/exposure
spot keys:    ['gold', 'silver', 'platinum', 'palladium', 'gsr']
spot.gsr:     58.5
spot.gold:    {'price': 4573.0, 'change_pct': 0.33}
spot.silver:  {'price': 78.17, 'change_pct': 0.94}
signal:       populated dict
```

Frontend renderer at `dashboard/static/index.html:34801-34803` reads
`spot.gsr` and writes `(spot.gsr || 0).toFixed(1)` into
`#metal-spot-gsr .metal-spot-price`. With spot.gsr=58.5 the panel
renders "58.5" — no longer "--".

## Resolution chain
- Round 3 ship: commit `21e7462` HM-DASHBOARD-CHROME-AUDIT-FIXES-ROUND-3
  shipped the metals_ledger wire as part of the Signal Center proxy
  last-good fallback bundle.
- Trader restart after that ship picked up the wire and the GSR cell
  has been live since.

## Decision (per draft Item 6 decision tree)
**(a) GSR rendering correctly post Round 3 restart → close the ticket
as auto-resolved by metals_ledger ship.**

No code changes in Round 5. This file documents the chain so future
audits don't re-discover and re-bank the same symptom.

## Implication for related banked items
HM-INSIDER-FEED-INIT-STUCK in the original 2026-05-18 bank file was
noted as a potential same-class victim of HM-SIGNAL-CENTER-PROXY-NULL-CACHE.
Round 3 also addressed proxy null-cache. Insider Activity panel state
not re-verified this round — keeping its banked-item open until next
Captain page dump confirms or denies the same auto-closure.

## Cross-refs
- `reports/BANKED_ITEMS_2026-05-18.md` (HM-METALS-GSR-DATA-WIRE entry)
- `reports/BANKED_ITEMS_2026-05-18_DASHBOARD_AUDIT.md` (source of the
  HM-ROUND-5-MEGA item list)
- commit 21e7462 (the auto-closure ship)
