# CARRIER RUNG 2 — CONTEXT-ON-ARRIVAL · DESIGN SPEC
**Status: DESIGN ONLY — not built. Builds on Rung 1 (carrier_rung1_contact.html).**
**Doctrine: read-only; existing /api/* only; no new endpoints; no execution.**

## Goal
When a CONTACT is acquired (Rung 1), the card auto-fills the surrounding NOW-edge so the operator can *orient* without leaving the view — the CIC sensor picture for one contact. Rung 1 already stubs six context slots with `data-endpoint` hints; Rung 2 wires them.

## Hard constraints
- **Read-only.** Every slot is a GET against an endpoint that already exists and works on v1 (confirmed live in the parity audit). No backend changes, no new routes.
- **Colorblind doctrine:** blue/amber only, numbers always shown. Direction/sentiment never encoded by red/green — use amber=elevated, blue=baseline, plus the number.
- **Fail soft.** A slot whose endpoint errors or is empty shows "no data" in the interface's voice, never a spinner that hangs or a blank. One dead slot must not break the card.
- **Freshness on every slot.** Each fetched value carries an age (seconds/min). Stale data is shown *with its age*, not hidden — the operator decides.

## Slot → endpoint map (all confirmed live on v1)
| Slot | Endpoint | Render |
|------|----------|--------|
| Last / Mark | `/api/price/{sym}` | last, %chg (number + amber if >±2%) |
| Options Flow | `/api/market/flow` (filter sym) | net premium, put/call, top sweep |
| Congress / Insider | `/api/congress/{sym}` | most-recent disclosed buy/sell + date |
| Gamma / GEX | `/api/gex-snapshot` (filter sym) | net GEX, flip level vs spot |
| Live Tape | `/api/news/{sym}` or `/api/market/earnings` | latest 2 headlines + age |
| Fleet Read | `/api/signals` (filter sym) | fleet consensus + most-recent grade |

## Per-source emphasis (the card adapts to *why* it fired)
The firing source reorders/*highlights* slots — same data, prioritized to the contact's nature:
- **bk_avwap / bk_orb / bk_box** → lead with **Last/Mark** (the level) + **Options Flow**. Price-structure contacts.
- **uhura** → lead with **Fleet Read** (expand the 7-source confluence breakdown) + **Live Tape**.
- **congress / insider** → lead with **Congress/Insider** (expand: overlap, 13F if available) + **Last/Mark**.
- **options_flow / volatility** → lead with **Options Flow** + **Gamma/GEX** (flow + dealer positioning together).

Implementation: a `sourceEmphasis[src]` map returning an ordered slot list + which one renders expanded. Default order if source unknown.

## Fetch strategy
- Fire all slot GETs in parallel on `openContact()`, each independently rendered as it lands (progressive — don't block the card on the slowest).
- 4s per-slot timeout → "no data (timeout)".
- Cache per (sym) for ~30s so re-opening the same contact is instant; respect the freshness age display.

## Build checklist (when greenlit)
1. `sourceEmphasis` map + slot reorder/expand.
2. Six `fetchSlot()` functions (parallel, fail-soft, freshness-stamped).
3. Wire into Rung 1's `openContact()` after the card shows.
4. Self-verify: deep-link `?focus=NVDA&src=uhura` → Fleet Read leads, all slots resolve or fail-soft, ages shown.

## Explicitly NOT in Rung 2
- No write of any kind. No execution. No new endpoints. Rung 3 (debate-in-place) and Rung 4 (engage) are separate and gated.
