# Relay — A1: S8 season rotation executed. 2026-09-11

Executed during dry-dock, trader stopped (no restart race). Pre-rotation
snapshot captured (74 halted agents, 8 active) before the call.

`rotate_season(caller="s8-manual")` → returned `8`. Six-step sequence
fired as verified: dry-run scope check (`active_before=8, would_affect=8,
safe=True`) → S7 summary saved (81 players) → `current_season` 7→8,
`season_8_start` stamped → cash reset to $7000 for the 8 active agents →
unhalt scoped to `halt_reason IS NULL` (0 rows touched — nobody was
eligible, matching the dry-run) → positions cleared for AI players
(`webull`/`alpaca-mirror` untouched by design, `neo-matrix` also excluded
by design — `INDEPENDENT_PLAYER_IDS` in `shared/matrix_bridge.py`, 2 of
its positions correctly survived).

**Verified post-rotation:** the 74-agent halted list (`exit_only=3,
full=71`) is byte-for-byte identical before/after (`diff` clean) — no
halted seat was touched, no revive without a fresh rating. `current_season`
confirmed 8. Cash confirmed $7000 for all 8 active agents. Positions
confirmed cleared except the two documented exclusions.
