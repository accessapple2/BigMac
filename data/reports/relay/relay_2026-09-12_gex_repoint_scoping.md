# Relay — 2026-09-12. GEX repoint — scoped investigation, read-only, no build.

RULE #1 not implicated (no trades touched); no code changed either — this
is investigation only, per instruction. All four questions below.

## 1. The wall-label question, stated precisely

**Not a CBOE-data sign-convention issue. An internal producer/consumer
naming mismatch, already named once before and never resolved — just
routed around.**

**Consumer-side convention** (what every reader of `call_wall`/`put_wall`
assumes the names mean), shown on both sides:

- `engine/ready_room.py:211-212` hard-codes it structurally:
  ```python
  dist_call = (call_wall - spot) / spot * 100 if call_wall > spot and spot > 0 else 999.0
  dist_put  = (spot - put_wall)  / spot * 100 if put_wall  < spot and spot > 0 else 999.0
  ```
  and labels it explicitly at line 407/410: `"Call Wall (resistance)"`,
  `"Put Wall (support)"`. **`call_wall` must be above spot (resistance);
  `put_wall` must be below spot (support).** If a candidate value doesn't
  satisfy that, the code doesn't reject it — it silently falls into the
  `999.0` branch and reads as "irrelevantly far away."
- `gex_calculator.py:75-76` (Alpaca), the dataclass field comments say the
  same thing independently: `put_wall: float # support below spot`,
  `call_wall: float # resistance above spot`.
- `engine/options_flow_gex.py:287-294` (canonical/Polygon) *computes* it
  the same way: `call_wall` = strike with max positive call-GEX (no
  explicit filter, but calls concentrate above spot in practice);
  `put_wall` explicitly **constrained to strikes below spot**
  (`_puts_below = {k: v for k, v in put_gex.items() if spot and k < spot}`)
  — the code comment there says exactly why: *"a put wall is support
  beneath price; the unconstrained min could land above spot (deep-ITM
  puts)."* Three independent places agree on one convention.

**Producer-side (CBOE) convention** — `engine/gex_scanner.py:163,175`:
```python
gex_type = "call_wall" if m["net_gex"] > 0 else "put_wall"
```
That's the *entire* rule: whichever strike has the single biggest
**absolute** net-GEX magnitude gets picked as a magnet, then labeled
`call_wall` if its net GEX is positive, `put_wall` if negative —
**with no constraint on which side of spot it's on.** And the module's
own prompt-text labels (`build_gex_prompt_section()`, lines 257/264) call
`call_wall` **"(support/pin)"** and `put_wall` **"(resistance/
accelerator)"** — the literal opposite framing from every consumer above.

**This is a named, dated, prior incident, not a new discovery.**
`dashboard/app.py:6725` (the `/api/market/gex` handler) carries this
comment from **2026-06-09**:
> `HM-DRYDOCK A1 2026-06-09: repointed off legacy engine.gex_scanner
> (CBOE — stale spot, sign-based regime that contradicted Archer and
> wrong/collapsed walls) onto the SINGLE canonical GEX...`

So CBOE was already tried as a live source, already produced wrong walls
because of exactly this sign-based-no-spot-constraint labeling, and was
already routed around three months ago — not fixed, routed around. The
CBOE repoint that's been blocked since August is, as far as I can find, an
attempt to go back to a source with a known, previously-diagnosed defect
that nobody patched the first time.

**Live demonstration, run just now** (`get_gex('SPY', force=True)`,
today's real CBOE data, spot $764.29):
```
primary:   764.0 -> call_wall (net_gex +2.78B)
           760.0 -> put_wall  (net_gex -2.29B)
           750.0 -> put_wall  (net_gex -2.28B)
secondary: 765.0 -> call_wall, 755.0 -> put_wall, 740.0 -> put_wall
```
Today, by coincidence, every wall landed on the "expected" side of spot —
this run does **not** reproduce the 6/09 incident. But nothing in the code
*enforces* that outcome; it's a property of today's particular gamma
distribution, not a guarantee. The question was never "does CBOE's raw
gamma/OI data have a different sign than ours" — it's a plain physical
quantity, same sign everywhere. **The question is: does `gex_scanner.py`'s
labeling rule (sign-of-net-gex, no positional constraint) reliably produce
the same `call_wall`=resistance-above / `put_wall`=support-below meaning
every consumer already assumes — and the 6/09 incident plus today's own
code (no spot-relative filter, backwards prompt-text labels) both say no,
not reliably.**

## 2. Free-path inventory

### CBOE (`engine/gex_scanner.py::_fetch_from_cboe` / `get_gex`) — works, tested live

`GET https://cdn.cboe.com/api/global/delayed_quotes/options/SPY.json` —
**HTTP 200**, 13,228 contracts, spot $764.29 matched real SPY. Checked
specifically whether Greeks are actually populated near the money (a real
risk with CBOE's delayed feed — the sample row I first inspected was a
deep-ITM strike-500 contract with `gamma: 0.0`, which could have meant
"CBOE never populates gamma"): **468/468 contracts within $5 of spot carry
real, non-zero gamma and open interest.** The feed is genuinely usable
today, not a dead or degraded endpoint. Caveat inherent to the source, not
this test: it's CBOE's own **delayed** quote (the endpoint name says so),
not real-time.

### Alpaca (`gex_calculator.py::compute_gex_sync`) — works, tested live, better

Called it directly (`.venv/bin/python3`, the interpreter the live trader
actually uses — a plain `python3` first attempt failed with `No module
named 'alpaca'`, which is an environment mismatch, not a real finding).
**Succeeded outright, right now, market closed:**
```
spot_price: 764.22   (CBOE said 764.29 same moment — consistent)
call_wall (resistance above spot): 775.0
put_wall  (support below spot):    750.0
gamma_flip: 761.62
total_gex: -4,485,250,437.25
214 usable strike levels
source: alpaca
```
**Nothing is broken here.** `/api/gex/{symbol}` (the route you remember
serving Alpaca GEX in April) still exists — `dashboard/app.py:7008`,
literally still named `gex_alpaca()` — but was **deliberately repointed**
away from Alpaca to the Polygon-based canonical source on **2026-05-31**
(`HM-GEX-CANONICAL`, consolidating 3 disagreeing GEX displays into one).
`gex_calculator.py` itself was never touched or degraded; it was just
demoted to "dormant/preserved" status by that repoint, then happened to
outlive the thing it was repointed to (Polygon died 7/22, almost two
months *after* the Alpaca path was already sidelined). Re-testing it just
now proves the underlying capability was never lost — only its wiring
was, and on purpose.

## 3. Consumers — what reads GEX today, and what happened during the stale window

Builds on the existing trace (`relay_2026-09-01_gex-fossil-vs-live-path-
trace.md`) and today's Bridge Phase 1/2 staleness fixes — re-verified
live, not just re-read, before relying on either.

**The one real decision-path gate never saw stale data.**
`engine/risk_manager.py:797-813` — GEX regime check (25% position-size cut
on negative total GEX; hard block on a BUY within 1% of the call wall) —
reads `gex_calculator.get_latest_snapshot("SPY")` directly. That's the
Alpaca path, `data/trader.db`'s own `gex_snapshots` table — never touched
`data/flow_gex.db` (the frozen Polygon file) at any point. **Confirmed
live just now: this table is current, this gate has been reading real
numbers the entire time.** No gate consumed stale GEX for two months —
checked, and the answer is no.

**The fleet-wide LLM prompt injection was also never exposed.**
`engine/providers/base.py:919-927` (`build_alpaca_gex_prompt_section`) —
every AI player's context block gets an Alpaca-sourced GEX section, same
source as the gate above. Also clean throughout.

**What actually was stale for ~6 weeks (7/21 to 9/1), confirmed fixed
today:** `engine/ready_room.py` and `engine/dynamic_advisor.py`'s
canonical-overlay blocks used to apply the frozen `flow_gex.db` row
(spot, call_wall, put_wall, gamma_flip, king_node, total_gex — the
underlying SPY *price* included, not just the gamma levels) to the
Ready Room briefing and the human-facing "Troi's read" advisory panel,
unconditionally, with no age check. Shipped **HM-GEX-FRESHNESS-GATE-
2026-09-01** (verified live in the code just now, `ready_room.py:506-528`
and the mirrored block in `dynamic_advisor.py`): both now call
`engine.canonical_gex.canonical_gex_if_fresh()`, which returns `None` on
stale/missing/errored data, so the stale row now correctly falls through
to the same live Alpaca values the gate above uses. **As of today, every
consumer checked — gate, prompt injection, and both advisory panels —
effectively reads live Alpaca GEX, one way or another.** The dashboard's
`/api/gex-overlay/levels` and `/api/gex-overlay/heatmap` endpoints also
now carry the `stale`/`age_days` markers (verified present in
`dashboard/app.py`), closing the "3 more endpoints" gap the 9/1 doc
flagged as missed.

**Two currently-inert consumers of the CBOE path itself**, worth naming
since a repoint changes their exposure: `engine/spy_wall_strategy.py`
(via `crew_scanner.py`'s Gate 7 / `SCAN_PAIRS`) reads live CBOE walls
today — but that path is the one already found dormant this week (272
decisions, 0 executions; McCoy's real trades don't route through it).
`engine/dayblade.py`'s CBOE-based prompt injection feeds `dayblade-sulu`,
`halt_mode='exit_only'` — no new positions. Neither is exposed to real
capital right now, but both would inherit the wall-label question
immediately if CBOE became the fleet's canonical GEX source and either
path were ever reactivated.

**Bottom line for this question:** the thing you'd worry about — a live
gate silently mis-sizing or mis-blocking real trades off frozen data for
two months — didn't happen. What did happen was two human-facing display/
advisory surfaces rendering six-week-old numbers as current, which is
real and was worth fixing, and is now fixed.

## 4. Proposed repoint — Alpaca, not CBOE

**Pick `gex_calculator.py` (Alpaca) as the canonical source.** Reasoning,
plainly:

1. **It already works, verified live, with no defect to fix first.** CBOE
   requires building a fix for a named, three-month-old, previously-
   unresolved wall-labeling bug before it can be trusted as more than a
   display curiosity — Alpaca requires nothing.
2. **Real-time, not delayed** — CBOE's own endpoint name says "delayed
   quotes." For anything feeding a 0DTE-adjacent decision, that's a real
   difference, not a formality.
3. **Already funded, already the fleet's core provider** — no new
   subscription, no new dependency, no new outage surface. Consolidating
   onto Alpaca *reduces* provider sprawl; CBOE would add a third live
   provider (Polygon, Alpaca, CBOE) to a system whose own May doctrine
   (`HM-GEX-CANONICAL`) exists specifically to stop multiple GEX sources
   from disagreeing.
4. **It's already the de facto answer today**, just not by declared
   policy: the real trade gate and the fleet prompt injection have used
   it continuously and correctly this whole time; the display/advisory
   layer now reaches it too, via a "fall through to legacy" path that
   only exists because Polygon is dead. Declaring Alpaca canonical
   formalizes what's already true rather than introducing a third
   contender.

**What the repoint would concretely involve** (not built, proposal only):
- Make `engine.canonical_gex`'s priority order put the Alpaca snapshot
  first, with Polygon demoted to an optional secondary tier if that
  subscription is ever restored (rather than tier-1-by-default against a
  provider that's been dead two months).
- Leave `engine/gex_scanner.py` (CBOE) dormant, preserved, undeleted — per
  this repo's own convention — rather than investing in the label fix
  unless a specific future need for a second, independent cross-check
  source justifies it.
- Extend the same freshness-gate pattern (`canonical_gex_if_fresh()` or
  its Alpaca equivalent) to the Alpaca snapshot's own age, so a future
  Alpaca options-entitlement hiccup doesn't quietly repeat this exact
  incident a third time.

Nothing above has been built. Market's closed; this is scoping only,
awaiting direction.
