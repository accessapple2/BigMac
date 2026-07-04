# CARRIER RUNG 3 — DEBATE / REVIEW IN PLACE · DESIGN SPEC
**Status: DESIGN ONLY — DO NOT BUILD. Gated behind the realized-return rewire (P1).**
**Builds on Rung 1 (live, 21e8279) + Rung 2 (live, e44ec48).**

## Goal
From an acquired CONTACT, the operator can *interrogate* the setup in place — pull the war room's read, the scorecard, the fleet consensus — without leaving the contact view. This is the **TARGET** step of the kill chain: build a firing solution before anything launches. Still no execution (that's Rung 4, gated separately).

## Why this is design-only (the gate)
Two reasons Rung 3 is NOT an unattended build:
1. **It's interactive and stateful.** Unlike Rung 2's passive reads, Rung 3 can *invoke* a war-room run (agents deliberate on demand). That triggers compute and writes debate records — behavior that needs the Admiral's eyes on it, not an overnight job.
2. **It leans on the layer being rewired.** The war room's value rests on fleet/signal quality, and the per-source "edge" metric is currently contaminated (scanner projection, not realized return). Building a *decision surface* on top of a measurement layer mid-rewire is the "second floor before the foundation sets" anti-pattern. **Rung 3 build waits until realized-return (P1) lands** so the debate surfaces a true read, not a fake one.

## Interaction model (two modes)
- **PASSIVE (read existing):** show the most recent war-room debate / scorecard / consensus for this ticker if one exists. Read-only, could ship earlier — but still hold until P1 so it doesn't surface contaminated edge.
- **ACTIVE (invoke):** an explicit "Convene War Room" control that triggers a fresh deliberation on this contact. Stateful (writes a debate record), on-demand only, never auto-fired. This is the part that strictly requires Admiral review.

## What it shows (read-only surfaces, all existing endpoints)
| Element | Source | Note |
|---|---|---|
| Latest debate summary | `/api/ready-room/briefing` or war-room read | verdict + key dissent |
| Fleet consensus | `/api/signals` (sym) | agree/dissent count |
| Scorecard | `/api/ratings` (sym) | per-agent grade |
| Crew dissent | war-room read | the bear case, surfaced |

**Contamination guard (same as Rung 2):** no `fwd_return`/edge/alpha number renders until realized-return is live. Show the debate's *reasoning and consensus*, not a fake edge score.

## Active-mode design (the gated part)
- Control: "Convene War Room" button in the CONTACT card, below context (currently "Review in War Room" stub).
- On click: POST to the existing war-room run endpoint scoped to this ticker; show a live "deliberating…" state; render the verdict + dissent when it returns.
- **ROE:** invocation is operator-initiated only (never auto). It writes a debate record (append-only). It does NOT place any order — Rung 4 is separate and gated. Kill-switch unaffected.

## Dependency chain (explicit)
```
realized-return rewire (P1)  ──►  trustworthy per-source read
        │
        ├──►  Rung 3 PASSIVE (show real debate/consensus, not contaminated edge)
        └──►  Rung 3 ACTIVE  (convene war room — Admiral-reviewed build)
                                   │
                                   └──►  Rung 4 ENGAGE (gated: source must prove
                                         REALIZED edge before a sortie path exists)
```

## Build checklist (when greenlit, post-P1, with Admiral review)
1. PASSIVE surfaces first (read-only debate/consensus/scorecard), contamination-guarded.
2. ACTIVE "Convene War Room" — operator-initiated, append-only debate write, live state.
3. Self-verify: convene on a contact → verdict+dissent render → no order placed → kill-switch intact.

## Explicitly OUT OF SCOPE
- No execution / no order placement (Rung 4, gated on realized edge).
- No auto-invocation of the war room — operator-initiated only.
- No surfacing of contaminated edge metrics. Build only after P1 + Admiral review.
