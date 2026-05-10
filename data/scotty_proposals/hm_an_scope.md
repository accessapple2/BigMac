# Investigation 7 — HM-AN Scoping

**Filed:** 2026-05-10 by Scotty (loose-ends sweep)
**Status:** Ticket-it doc. Prior scoping already exists in `docs/SCOTTY_AUDIT_2.md` Section I. This file converts that recommendation into a `XO_BACKLOG.md`-ready entry.

## Background

The directive describes HM-AN as a "Morpheus reframe of port 9000" depending on HM-AM. `docs/SCOTTY_AUDIT_2.md` Section I (line 389) explicitly rejected that framing and recommended a different ticket shape.

## Current state of Signal Center (port 9000)

- **Process:** `com.trademinds.signal-center` launchd job (PID 18380 at session start). Bound to `127.0.0.1` per CLAUDE.md "Network Bindings" (HM-AW tracks reopening to network now that 2FA + RBAC are in place).
- **Producer surfaces in this repo:**
  - `engine/alpha_signals.py:5,1408,1478` — posts composite alpha to `:9000/api/signal`
  - `engine/brain_context.py:358,371,375` — OPTIONS_FLOW fire-and-forget
  - `engine/ai_brain.py:1313,1357` — high-confidence decision posts to `http://localhost:9000/api/signal`
  - `engine/ai_saas_disruption_scanner.py:11` — also posts to 9000
- **Engine-side consumers:** present (per `docs/SCOTTY_AUDIT_2.md` line 434 description: "a producer with engine-side consumers and zero dashboard consumers")
- **Dashboard consumers:** **zero** — the dashboard does NOT currently read from Signal Center at all. This is the gap.

## Why "Morpheus reframe" was rejected (SCOTTY_AUDIT_2 Section I)

Audit Section I concludes Signal Center is *"a producer with engine-side consumers and zero dashboard consumers. **Right action:** (a) keep server, (b) add `base_rate_features` rolling-window prune (retain last 6 months ≈ 75k rows; drops 643 MB to ~250 MB), (c) wire 2-3 read paths into the dashboard so it earns its name (Top-10 #4 candidate for HM-AN), (d) reject Grok's 'expansion' framing."*

## Proposed ticket (BACKLOG-ready)

```
## HM-AN — Signal Center → Dashboard read bridge

**Type:** Dashboard read-path wiring (cross-cutting)
**Priority:** P3
**Effort:** ~4h (per SCOTTY_AUDIT_2 Section I estimate)
**Depends on:** none (independent of HM-AM, which is shipped)
**Origin:** SCOTTY_AUDIT_2.md Section I, 2026-05-09

### Goal
Wire 2–3 dashboard read paths into Signal Center (port 9000) so the dashboard
shows what the producers are emitting. Today, the dashboard reads from
`trade_signals` and friends in `trader.db` — Signal Center sits idle from the
read side despite ~5 producer paths writing to it.

### Out of scope
- "Morpheus reframe" — verbal placeholder rejected by audit
- HM-AW network rebinding (separate; auth-gated)
- New producer wiring (this is purely consumer-side)

### Scope (small)
1. Identify the 2–3 highest-signal endpoints Signal Center exposes (audit
   `signal-center/server.py` for `/api/*` routes).
2. Add corresponding read-path tiles or rows in `dashboard/static/index.html`
   (or via `dashboard/app.py` proxying — TBD by smaller of the two).
3. Verify NTFY/log shows reads landing.

### Risk
- **Sacred DB:** none — Signal Center has its own DB at `signal-center/signals.db`.
- **Service impact:** new dashboard endpoints only; no schema changes.
- **Rollback:** `git revert` the dashboard wiring commit.

### Cross-references
- `docs/SCOTTY_AUDIT_2.md` Section I (origin)
- `engine/signal_poster.py::post_to_9000` (producer pattern)
- `dashboard/static/index.html` (consumer surface)
- CLAUDE.md "Network Bindings" (HM-AW context)
```

## What NOT to do (per directive's discipline)

- Do **not** rename to "Morpheus." Verbal placeholder, no consensus.
- Do **not** expand to "absorb Signal Center into engine." Grok's "expansion" framing rejected.
- Do **not** ticket as dependent on HM-AM. HM-AM is shipped; HM-AN is independent.

## Recommendation

File the BACKLOG-ready ticket text (above) into `docs/XO_BACKLOG.md` under "Ranked Improvements / Open Sprints" or wherever the audit's Top-10 list lives. Do NOT start the work this session.
