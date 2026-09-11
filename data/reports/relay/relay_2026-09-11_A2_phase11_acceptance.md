# Relay — A2: Phase 1.1 acceptance read. 2026-09-11

`scripts/mccoy_invalidation_acceptance.py` (today, 2026-09-11 trading day
so far): **919/971 directional signals plausible (94.6%) — PASS** against
the 90% bar.

**Stop-distance distribution** (% invalidation is from reference_price,
n=971, all parseable): min=0.00, p10=0.95, p25=1.61, **p50=2.52**, p75=3.67,
p90=5.40, max=21.33. mean=2.93, stdev=2.01. 95.3% land in the 0.5-15%
plausible band; 4.6% (45) are under the 0.5% floor (too tight — this is
the dominant failure mode, not wrong-direction); 0.1% (1) over the 15%
ceiling (a genuine outlier, $10.92 vs $13.88 ref). No systematic bias —
healthy, centered distribution, not clustered at either edge.

**Verdict: clears the bar on both pass rate and distribution shape.
Proceeding to A3 — no prompt iteration needed.**
