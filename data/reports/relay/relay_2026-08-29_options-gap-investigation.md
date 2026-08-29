# Relay — Options-Trade Gap Investigation (2026-08-29)

Full report: `docs/reports/options_trade_gap_investigation_2026-08-29.md`.
Analysis only, no live changes, per directive.

## Headline finding (new, not previously known)

`logs/trader.log` and the `trades` table both confirm a **33-day
fleet-wide trading outage, 2026-07-23 → 2026-08-24** — zero log lines,
zero trades, from any of ~78 active agents, for the entire window. Not
previously documented anywhere in XO_BACKLOG.md or prior relay reports.
Recovery since 08-24 looks unstable, not solid: restarts logged at 08-27
10:42 and 08-28 20:08, and the currently-running process has only 79
minutes of uptime as of this writing (another restart happened today,
not yet in the restart log). **This is the actual root cause behind most
of the six options pathways' inactivity** — bigger in scope than "the
options desk is idle," and it also explains why the GEX collector and
source-health watcher (OPS TRIAGE items today) died in the same window.
Flagged as a priority-0 follow-up, not solved in this pass.

## Per-pathway verdict (full evidence in the report)

| Pathway | Verdict |
|---|---|
| Troi wheel (`options-sosnoff`) | (b)+(c): VIX<18 gate blocked 38/39 scans since 07-06; also silenced by the outage |
| McCoy BPS (`mccoy-bps`) | (a): permanently frozen `insufficient_data`, zero activity ever |
| Anderson BCS (`anderson-bcs`) | (a): same, permanently frozen |
| Quark IC (`quark-ic`) | (a): same, permanently frozen. **Correction:** actual audition bar is 20 trades, not 50 — no 50-close threshold found anywhere in code for Quark |
| CSP shadow qwen3.5 (`shadow-qwen35-csp`) | (b)+(c): 3/30 era-closes, stalled 40 days |
| CSP shadow plutus (`shadow-plutus-csp`) | unclear — zero rows ever, may never have run |
| 0DTE dayblade (`dayblade-0dte`/T'Pol) | (b): already-diagnosed pricing bug, halted 07-13, fix pending — same treatment as GEX collector |

**Graduation timeline:** at the literal current close rate, shadow-qwen35-csp
never reaches 30 (0 new closes in 40 days); an optimistic historical-rate
extrapolation lands ~2026-12-24 but shouldn't be trusted given the VIX
regime. Quark IC is unconditionally "never" at any N — 0 signals in its
entire history, and the audition mechanism is documented to freeze
permanently once `halt_mode='full'`. Confirms the directive's premise:
the graduation gates have no activity floor.

## OllieTrades Signal impact

Live registry has 4 playbooks (not 3): `bull_put_spread` + `leveraged_put`
(options), `ollie_live_swing` (stock), `bear_play` (matches either).
Since the fleet's 08-24 recovery, only 3 of ~78 active agents have
emitted *any* signal at all, and zero of those are options-typed — the
last options-typed signal from anyone was 2026-07-21 (`ollama-plutus`,
the day before the outage). Troi herself is currently rating `N/A` (0
season-7 stock trades) so she can't be a "winning model" regardless.
**Conclusion: the two options playbooks are currently structurally
unreachable** — a direct downstream consequence of the fleet outage, not
a Signal-gate design flaw.

## Recommendations (proposed, not implemented)

1. Priority 0: dedicated root-cause investigation into the recurring
   trader-process instability (33-day outage + repeated recent restarts).
2. Shadow CSP graduation gate: add a stale-audition signal or a
   VIX-independent low-vol entry tier (Admiral call).
3. McCoy BPS / Anderson BCS / Quark IC: pick one of two honest end-states
   — reactivate into a non-executing shadow/sim tracking loop so audition
   data can actually accrue, or write a dated permanent-retirement
   decision in XO_BACKLOG.md so the frozen weekly proposal stops spinning.
4. Reconcile Quark's real audition bar (20 in code) against the 50 cited
   in the directive, if 50 was meant to be authoritative.
5. Signal: no gate change proposed — options playbooks should re-check
   themselves once fleet-wide signal generation recovers from item 1.
