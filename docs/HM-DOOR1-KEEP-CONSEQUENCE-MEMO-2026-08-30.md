# Door-1 KEEP Consequence — Case Memo

**Status:** Door-1 KEEP is closed and stays KEEP (G1-G3 passed on the original
2026-07-24 window, `HM-DOOR1-OLLIE-MACHINE-KILLGATE-VERDICT`, no re-score,
not reopened). This memo is scoped to one open question only: does the
KEEP branch's consequence — "halt all other strategies permanently" — apply
today? Ledgered as **PENDING RE-ARGUMENT**, not applied, not waived.
Due before Tuesday 2026-09-01 open.

## 1. What "other strategies" meant on 07-24

Verbatim, `OLLIETRADES_KILL_GATE.md`, pre-committed 2026-06-19, unedited since:

> **KEEP branch**
> G1 + G2 + G3 all pass:
> - Scale CSP book, halt all other strategies permanently
> - Re-run this gate every 30 days (rolling)
> - Hardware stays; return window is irrelevant

Every book in scope is **Alpaca paper**, always — Door-1, like the rest of
the fleet, never touches Schwab (RULE #1). "Options vs equity" is not a
carve-out in the text; read literally, "all other strategies" includes both.

The actual same-session action (`docs/door1-cut-2026-06-19.md`, 06-19) did
**not** halt all non-CSP activity — it kept 6 profitable non-CSP producers
active alongside CSP (options-sosnoff): McCoy, Worf, Capitol Trades, Trip
Tucker (energy-arnold), Neo, CTO Grok 4.2 — and only cut negative-realized
or zero-trade agents. So the KEEP branch's later "halt all other strategies
permanently" is a materially larger action than anything Door-1 itself did
at inception.

## 2. What those slots are NOW (S7)

Fleet is Season 7 (`settings.season=7`). Of the 6 non-CSP producers kept
alive on 06-19, three are still active today, three are already halted —
independent of Door-1, via ordinary season churn:

| 06-19 producer | Today | How |
|---|---|---|
| McCoy (ollama-plutus) | **active** | — |
| Worf (qwen3-8b-flash) | **active** | — |
| Capitol Trades | **active** | — |
| Trip Tucker (energy-arnold) | halted | fleet_lifecycle backfill, 2026-08-29 |
| Neo (neo-matrix) | halted | fleet_lifecycle backfill, 2026-08-29 |
| CTO Grok 4.2 (cto-grok42) | halted | fleet_lifecycle backfill, 2026-08-29 |

Current full active roster (10 seats): `capitol-trades`, `desk-manual`,
`enterprise-computer`, `m5-allocator`, `ollama-plutus`, `ollama-qwen3`,
`options-sosnoff` (CSP itself), `qwen3-4b-audition`, `qwen3-8b-flash`,
`trade-desk`. `desk-manual`/`trade-desk` are the Captain's own manual desk
(`TRADE_DESK_BYPASS_GATES=True`, human-directed, not an AI strategy) and
`enterprise-computer` is a cash bucket, not a trader — same three
non-strategy seats the 06-19 cut also declined to touch. `m5-allocator` is
a rebalancer across existing books, not an independent signal source.
`qwen3-4b-audition` is an audition seat mirroring Worf. The "options
engine" beyond CSP — `strategy:bull_spread_v1` — has been dormant since
2026-07-13; `swingdesk-manual` (also manual, not AI) dormant since 07-22.
`ollie-machine` is out of scope here — separate gate, already `full`
(ledger 114, review-by 09-29), leave it.

**Real candidate set for "other strategies" today: 6** — ollama-plutus,
qwen3-8b-flash, capitol-trades, ollama-qwen3, m5-allocator,
qwen3-4b-audition.

## 3. Which sentence was it — crowd out, or keep running?

The written text is unambiguously the first: **"halt all other strategies
permanently"** is an instruction, not a description of CSP's own survival.
It is not sloppy shorthand for "CSP earned the right to keep running" —
that would have needed no object beyond CSP itself; this sentence has an
explicit object (everyone else) and an explicit verb (halt). Taken at face
value, it is a crowd-out order.

But the surrounding text ("hardware stays; return window is irrelevant")
frames the whole gate around one question: does OllieTrades as a project
justify the MSI hardware, not being returned to Costco. The bleed-isolation
cut done the same session (06-19) treated "CSP proved itself" and "cut
everything else" as two different, independently-argued decisions — profit,
not book identity, was 06-19's actual cut criterion, and it explicitly kept
6 profitable non-CSP books running next to CSP. The KEEP-branch sentence
reads as the harder claim on paper; the same-day precedent it sits next to
was never applied that harshly in practice.

## 4. Current P&L / DD / hit rate — last 20 sessions, not July

There aren't 20 recent sessions to report. The fleet was fully dark
2026-07-22 → 2026-08-29 (the same stand-down under review all night).
Since 2026-07-15 (last pre-quietdown data through today):

| Strategy | Trades since 07-15 | Realized P&L | Last trade |
|---|---|---|---|
| ollama-plutus (McCoy) | 0 | — | none since 07-15 |
| qwen3-8b-flash (Worf) | 0 | — | none since 07-15 |
| ollama-qwen3 (Dax) | 0 | — | none since 07-15 |
| qwen3-4b-audition | 0 | — | none since 07-15 |
| capitol-trades | 3 | -$116.89 | 2026-07-16 |
| m5-allocator | 10 | +$17.49 | 2026-08-27 |
| options-sosnoff (CSP itself, for scale) | 0 CSP entries | — | last exit 2026-07-20 |

DD is not computable — nothing has held a position long enough since
revival to draw one down. **CSP itself has been exactly as dormant as
everything else** — zero new CSP entries since 2026-06-29.

## 5. Blast radius

**Would actually stop** if applied literally: ollama-plutus, qwen3-8b-flash,
capitol-trades, ollama-qwen3, m5-allocator, qwen3-4b-audition — 6 seats,
none of which are currently doing anything (§4). `desk-manual`/`trade-desk`
(manual, bypasses agent gates by design) and `enterprise-computer` (cash
bucket) would not be "strategies" to halt under the same reading 06-19
used.

**Keeps its own kill switch regardless, untouched by this decision either
way:** RULE #1 Schwab hands-off (already independent of the paper fleet);
the 20% drawdown auto-halt in `risk_manager.py::check_drawdown()` (per-agent,
transient, fires on its own); the HM-DOOR1 leveraged-ETF CSP blocklist
itself (permanent, unrelated to this consequence question, stays in force
either way); `ollie-machine`'s halt (ledger 114) — separate gate, not
touched by anything decided here.

## 6. Recommendation

**Waive, with note.** The decision rule this memo is arguing against: a
late halt needs a present reason — it fires only if current data shows the
other books are eating Door-1's edge. There is no such data. There is
barely any data at all — the candidate strategies have been dormant since
mid-July, CSP included, for the same stand-down reason, not because of any
competition between them. Applying a fleet-wide halt now would be enforcing
a 07-24 threat against 08-30 silence, with zero evidence the two books were
ever actually contending for the same capital or compute in the interim.

If the Admiral wants a narrower path instead of a full waive: **apply-narrow**
is available — none of the 6 candidate seats currently need active
intervention (all dormant), so apply-narrow and waive-with-note produce the
same practical state today; the only difference is a standing order for
what happens if/when these seats resume trading. Recommend waive-with-note
and revisit if any of the 6 shows real activity that measurably competes
with CSP — not on a fixed date, on an actual signal.
