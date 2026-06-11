# Session: Tailnet Dedup + HM-FORGE Phase 3 + Coexistence Verify
## Date range: 2026-06-10 (single session, ~3 topics)

## Key decisions
- **Tailscale:** keep the homebrew `tailscaled` (node `bigmac`, 100.95.222.119) — a root LaunchDaemon that survives SSH-only reboots; remove the macsys GUI/system-extension node (`steves-mac-mini`, 100.124.131.19). The GUI variant is login-tied and headless-fragile.
- **WR witness model:** raw bake-off winner was `gpt-oss:20b` but it evicts the live fleet (12.5GB on a 16GB GPU, ~every 18min). Admiral chose **`gemma4:12b-it-qat`** (7.4GB, co-resides with plutus-v1 4.6GB = 12GB) under a new **VRAM-co-residency eligibility gate**. `gpt-oss:20b` parked as the vLLM-PoC solo-GPU candidate.
- **Phase 1.5 gate:** the "PARTIAL" status was a `grep -c` bug, not a real failure; Admiral cleared the gate to proceed.
- Witness is **report-only**, non-blocking (background thread), `keep_alive=0`, never enters the vote.

## Code changes / files touched
- `engine/war_room.py`: witness A/B (gemma4↔plutus-v1), additive `war_room_debates.{witness_model,witness_take,witness_wall_s}`, `_record_witness` in a daemon thread; hook after round close.
- `engine/providers/ollama_provider.py`: backward-compat `keep_alive` param (default "10m").
- `scripts/hm_forge_phase15.sh`: fixed `grep -c || echo 0` exit-1 double-"0" → false PARTIAL.
- `drafts/HM-FORGE-PHASE4-AB-SCORECARD-SPEC.md`: added §2a eligibility gate, gpt-oss park note, §9 reconciled to `war_room_debates`.
- Crontab: installed `*/5` watchdog supervisor (backup archived to `archive/crontabs/`).
- Committed `ab222a0`, pushed.

## What worked / what didn't
- **Worked:** isolated `ollama ps` co-residency test (gemma4+plutus, no eviction); witness verified end-to-end + 2-debate alternation dry-run; SC↔trader handshake (correct PIN→200, wrong→401).
- **Didn't:** non-root Tailscale CLI surgery — `down` hit the wrong daemon (dropped bigmac), restored immediately. macsys removal is SIP-blocked → needs GUI. First witness call cold-timed-out at 60s → fixed with bg thread + 120s timeout.
- **Stale-doc traps:** Phase-1.5 PARTIAL, `_EXECUTION_ENABLED "must be False"` (actually intended True since `df7320c`), `is_halted` column (dropped HM-B). Verify live state, not docs.

## Status as of conversation end
- **HM-FORGE Phase 3: SHIPPED** (`ab222a0` pushed, unpushed=0). Trader pid 38768, SC 37849, watchdog 71823 all healthy. A/B window opens at next market open.
- **Tailscale dedup: BLOCKED** — needs at-machine GUI (trash Tailscale.app → approve extension removal; app pre-archived to `~/Tailscale.app.archive-2026-06-10`).
- **Coexistence verify: PASS** — HM-FORGE built additively on Drydock; no overwrites, halts intact, gate True-by-design.

## Open threads
- Tailscale: finish GUI removal at machine; canonical road address `100.95.222.119`; NTFY not yet sent (held until verified).
- No `@reboot` lines in crontab — signal-center/cloudflared reboot-survival looks unaddressed (watchdog covers trader only).
- `docs/S7_THESIS_2026-04-26.md` never committed — recover from Drydock scratch if real.
- Healthcheck stays OFF until `restart_server()` calls `trader_restart.sh` (return-trip ticket).
