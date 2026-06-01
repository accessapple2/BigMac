# Removed-model sweep + movers verdict (read-only) — 2026-06-01

## CRITICAL: inference host is .168, NOT localhost
config.OLLAMA_URL/OLLIE_URL = http://192.168.1.168:11434 (Ollie Max). Both CTO and the fleet
(ollama_provider via agent_routing, url from config) hit .168. `ollama list` on localhost is the
WRONG host — model presence MUST be checked on .168.
- **.168 installed:** gemma3:4b, ministral-3:3b, plutus-v1:latest, qwen2.5-coder:7b, qwen3:8b
- `_resolve_model_id` (agent_routing:60) returns ai_players.model_id **literally** — no alias remap,
  so a model_id not on .168 = a real 404 on inference.

## Silently-broken candidates (active/exit_only, model NOT on .168) — VERIFY, don't fix
| model_id | halt_mode | n | note |
|---|---|---|---|
| devstral-small-2 | active | 1 | also the CTO killer (now fixed). Not on .168. |
| deepseek-r1:14b | active | 1 | not on .168 (localhost-only). |
| qwen3:14b | exit_only | 2 | .168 has qwen3:**8b**, not :14b. |

Whether each fails SILENTLY depends on its caller's error handling (CTO swallowed → fixed; others
TBD). Next step: trace each agent's invocation + check if it swallows like CTO did.

## NOT broken (on .168 — earlier localhost sweep WRONGLY flagged these)
qwen3:8b (6 active), ministral-3:3b (4 active), qwen2.5-coder:7b (1 active), gemma3:4b, plutus-v1
(→ plutus-v1:latest). All present on .168.

## Intentionally-halted (halt_mode='full') — missing model EXPECTED, not a concern
gemma3:27b-it-qat, gemma4:26b/31b/e4b, llama4:scout, qwen2.5:7b, qwen3-coder:30b, qwen3.6:27b/35b-a3b,
qwen3:8b(4 full), ministral-3:3b(9 full), devstral-small-2(1 full), etc. Benched; do not revive.

## Movers poller verdict: RE-HOME, do NOT retire
`scrapers/polygon_movers.py` writes `data/trader.db::mover_watchlist`, which the dashboard scanner
READS (app.py:1587-1997 — price/pct_change/rel_vol fallback chain). On-demand `/api/movers`
(movers_cache.json via get_market_movers) is a SEPARATE path that does NOT cover mover_watchlist.
So the poller is NOT redundant. It's a gui/501 LaunchAgent, currently NOT loaded (reboot-gap) →
mover_watchlist is stale now (scanner price fallback degraded). **Flag for GUI-session rehome (or
@reboot cron wrapper like trader/signal-center); do NOT force from SSH (can't reach gui/501).**
