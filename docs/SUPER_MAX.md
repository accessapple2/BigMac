# SUPER_MAX.md

> Relocated from CLAUDE.md by HM-PRIME Part C (move, not delete).

## SUPER_MAX Wave Program (W0–W4, shadow-first; added 2026-06-03)

Staged research→graduation pipeline. Every wave is OBSERVATION-FIRST: candidate edges
emit as SHADOW, accrue forward, and graduate to execution ONLY on the validation gate
+ explicit Admiral go. Execution stays OFF until both are satisfied.

### Graduation gate (load-bearing)
No setup is proposed for execution until it clears DSR ≥ 0.95 AND PBO ≤ 0.30
(strategies/validation.py) on FORWARD (true-OOS) sample — never raw Sharpe, never
in-sample alone. PBO needs a non-degenerate config universe (a 2-setup matrix is a
coin-flip artifact). W4 bucketing shrinks n → PBO matters MORE, not less.

### Shadow boundary (hard)
Shadow signals carry agent='shadow-bridge:<setup>' and are excluded from execution by
construction: (1) the only trade_signals→buy consumer (neo-matrix) is exit_only, and
(2) a defensive guard skips agent_name.startswith('shadow'). They exist for W0 forward
scoring only; emission never forwards to Alpaca.

### The waves
- W0 — forward-scoring substrate: signal_outcomes (signals.db) ⟷ trade_signals 1:1 by
  signal_id; expectancy_engine.score_backlog() = stop-first R @1/3/5/10d, IS/OOS split.
  Lead: relative_strength (in-sample DSR ✓, n=444). PBO = 0.6348 FAIL (genuinely fragile,
  confirmed 2026-06-01 on 144-config decorrelated grid — do not graduate). bull_flag +
  rsi_bounce + rsi_divergence accruing. No-level signals are unscoreable.
- W1 — source health: engine.source_gate + /api/sources/health (signal-center :9000) →
  RED-first grid (signal-center/index.html). Auto-quarantine tracker is report-only
  (AUTO_QUARANTINE_ENABLED default-off); independent cron watcher
  (scripts/source_health_watcher.py, NTFY topic ollietrades-admin) is the live alerter.
- W2 — bracket sizing: SPEC only (drafts/). Gated AFTER graduation. Fixed-fractional →
  ≤0.25× Kelly; observation-only sizing log, never buy().
- W3 — gamma mapper + unusual-OI: SPECs only. Canonical GEX = /api/gex-snapshot (trader
  :8080, SPY/QQQ); unusual-OI from flow_gex.db flow_aggregates. Print-level flow is
  Polygon-tier-blocked (403) — an Admiral cost decision, not a build.
- W4 — regime-conditional routing (capstone): signal_regime sidecar (signals.db) stamps
  gamma_sign (from the GEX regime LABEL, not raw total_gex sign) × VIX term (vix_monitor)
  × time-of-day on every shadow signal via signal_bridge._stamp_regime. Router built only
  once per-(setup × regime-bucket) sample clears the gate.

### Build discipline (this program specifically)
Verify-before-build: every wave spec assumed a dependency that was stale, already-built,
or mis-shaped (W1 grid already shipped; 'iso' ts_format unsupported by source_gate; gamma
sign inverted vs the engine label; expectancy_engine can't read context_json). Confirm
CURRENT code/DB state via SQL/grep/live-probe before scoping any wave.
