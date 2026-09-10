# Question: what does "Door1 expiry fix" actually mean to build?

**Date:** 2026-09-10
**Context:** Backlog line `docs/XO_BACKLOG.md` — "Door1 expiry fix |
decision | unslotted | awaiting Door 1 kill-gate verdict context." The
verdict context is resolved (`HM-DOOR1-OLLIE-MACHINE-KILLGATE-VERDICT`,
rendered 2026-08-30). Instruction this morning: "implement as specified."

**Problem: nothing in the repo specifies it beyond one sentence.**
Searched every doc, script, and the backlog itself for "door1" + "expir*"
and "kill-gate" + "expir*" — the only hit is one line in
`docs/HM-DOOR1-KEEP-CONSEQUENCE-MEMO-2026-08-30.md`, written after the
memo waived the KEEP branch's "halt all other strategies permanently"
consequence for being invoked 37 days late on stale data:

> "This waiver retires THIS consequence instance only — any future gate
> wanting crowd-out semantics must state it explicitly with a
> data-freshness condition (consequence expires if not rendered within N
> days of window close), so no standing order can fire stale again."

That is a design principle for **future** gates, not a spec for a
concrete change: no N is chosen, no target file is named, no mechanism
(prose convention vs. code-enforced check) is picked.

**The obvious target file is locked.** The gate this memo is about,
`OLLIETRADES_KILL_GATE.md`, opens with: "Pre-committed 2026-06-19. Do not
edit after DAY 0 (~2026-06-24). Gates are pass/fail, not negotiated." DAY
0 passed three months ago. Editing it to retroactively add an expiry
clause would violate the exact integrity property that made this gate's
verdict trustworthy in the first place — the whole point of a
pre-committed gate is that it can't be amended after the fact, which is
why the memo phrased the fix as "future gates," not "this gate."

**Not blocked on anything except which of these you mean:**

## Options

- **New standing convention, documented, not code-enforced (Recommended)**
  — write a short doctrine note (in `docs/DOCTRINE.md` or `CLAUDE.md`,
  wherever load-bearing rules live) stating: any future pre-committed gate
  with a consequence clause must include an explicit expiry — "if this
  verdict/consequence isn't acted on within N days of being rendered, it
  requires re-argument, not automatic enforcement." `OLLIETRADES_KILL_GATE.md`
  itself stays untouched (respects its own no-edit rule); this only binds
  gates written from now on. Cheap, low-risk, matches how this repo
  documents doctrine elsewhere.
- **Code-level check** — build something that actually tracks
  verdict-rendered-date vs. consequence-applied-date and flags/blocks
  stale application automatically. Bigger: needs a place to store
  verdict/consequence metadata (no such table exists today —
  `debate_history`/`risk_assessments` aren't shaped for this), and a
  concrete N. Worth it only if more gates like this are actually planned.
- **Something else** — if "Door1 expiry fix" meant a specific different
  thing (e.g., an actual timeout on the Door1 leveraged-ETF CSP blocklist
  itself, not the KEEP-consequence question), say what — nothing in the
  repo points at that reading, so it'd need to be specified fresh.

No code or doc change made yet pending which of these (or what N) you
mean.
