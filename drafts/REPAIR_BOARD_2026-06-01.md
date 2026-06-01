# REPAIR BOARD — 2026-06-01 (single-terminal, sequential)

RAILS: frontend BUILT but HELD for Admiral browser smoke (never shipped); all backend
batched into ONE `./scripts/trader_restart.sh` at the end → verify shadow boundary
(chokepoint live, 45 shadow signals intact, 0 shadow-originated trades) + season/Troi
green; sacred-data; revert-on-failure. Report shipped vs held vs scoped.

## STATE LEGEND: ⬜ todo · 🔨 building · ✅ done(committed) · 🟡 HELD(smoke) · 📋 scoped · 💤 summarized

### BUILD NOW
- ⬜ 1. PBO config matrix — relative_strength param-grid × universe, .venv-backtest, observation-only → report real PBO
- ⬜ 2. #2 0DTE min-premium entry gate + absolute-dollar stop (backend, verify execution path)
- ⬜ 3. Display backend (batch→restart): bridge-Kirk as_of/live VIX-F&G · scanner MU/DELL col mis-map · fleet-count doc 20→21
- ⬜ 4. #8 crew dormant-drawer (collapse empty-E) — frontend HELD
- ⬜ 5. #9 LiveChart MutationObserver guard — frontend HELD
- ⬜ 6. W1 frontend health grid (real /api/sources/health + as-of + NTFY auto-quarantine) — frontend HELD

### RESTART GATE
- ⬜ ONE restart → boundary verify + season/Troi green

### SCOPE-ONLY (hold for go)
- ⬜ 7. #10 DOM lazy-load refactor (~100-section page) — scope only
- ⬜ 8. Daemon graveyard re-home commands (signal-center @reboot+disable, Kirk producer, ~24 agents) — scope only

### SUMMARIZE (green-light, don't build)
- ⬜ 9. 4 specs: W2 sizing, W3 gamma mapper, W3 unusual-OI, W4 routing — one paragraph each

---
## LOG
(checkpoints appended as work proceeds)
