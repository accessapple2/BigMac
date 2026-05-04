# HM-O — Ollie Box Ollama Health Check
*2026-05-04 afternoon, Scotty diagnostic*

## Verdict

**Scenario D — Box unreachable entirely.** Network-level failure, not an Ollama service issue. No remote fix attempted (per runbook: power/network issue is not a Scotty problem).

## Probe results

### Ping
```
ping -c 3 192.168.1.166
Request timeout for icmp_seq 0
Request timeout for icmp_seq 1
3 packets transmitted, 0 packets received, 100.0% packet loss
```

### Port 11434
```
nc -zv 192.168.1.166 11434
nc: connectx to 192.168.1.166 port 11434 (tcp) failed: No route to host
```

"No route to host" (rather than "connection refused") confirms it's a network-layer problem, not a stopped Ollama service. The trader's TCP stack can't even establish a path.

### `/api/tags`
Curl returned empty body in 5s timeout — consistent with "no route to host" returning fast. Per-model probes skipped (would all fail identically).

### Trader log evidence
`tail -200 logs/trader.log` shows a steady stream of `HTTPConnectionPool(host='192.168.1.166', port=11434): Max retries exceeded / NewConnectionError` errors. Multiple agents affected — coder is not the only one.

### Latest signal per Ollie-Box-routed agent
| Agent | Signal | Confidence | Time |
|---|---|---|---|
| ollama-qwen3 | HOLD | 0.0 | 2026-05-04 16:01:47 |
| ollama-coder | HOLD | 0.0 | 2026-05-04 15:42:03 |
| ollama-plutus | HOLD | 0.0 | 2026-05-04 14:58:24 |
| ollama-llama | HOLD | 0.5 | 2026-05-01 19:37:15 |
| ollama-local | BUY | 0.65 | 2026-05-01 13:30:49 |

The three currently-scheduled Ollie-Box agents (qwen3, coder, plutus) are all emitting connection-error HOLDs in real time. The qwen3 timestamp is **7 seconds after the start of this probe** — the trader is actively retrying and failing every cycle. The other ollama-* agents have older last-emit timestamps because they're not on today's active roster, not because they're working.

## Action taken

**None.** Per Scenario D rules, network-layer outage of a remote box is out of Scotty's scope. The Admiral / on-site captain needs to physically check power + network on Ollie Box before any service restart is meaningful.

## Follow-up

- 🚨 **Active gate-flip soak with degraded fleet:** the three Ollie-Box agents are emitting `HOLD, confidence=0.0` rather than no signal at all. These rows land in the `signals` table and may pollute downstream confidence aggregates. Worth checking that gate-flipped strategies (`bull_call_spread_v1`, `bear_put_spread_v1`) don't read these as "low-confidence HOLD" instead of "agent down".
- The signals from coder/plutus have been stale-error since at least 14:58 today — the outage has been going on for **~67 minutes** as of probe time. Worth correlating against any environmental change (router reboot? power blip? VPN state?).
- The trader keeps retrying every cycle — no exponential backoff observed. Mild waste of cycles but not urgent. Could be a future HM-X "circuit breaker for unreachable Ollama" item.
- When Ollie Box is back up, verify all three models load (`qwen3:8b`, `qwen2.5-coder:7b`, `0xroyce/plutus:latest`) — only one box-level outage was diagnosed; per-model state was not probed.

## Recommended next session prompt

> "Captain — Ollie Box is back online. Re-run HM-O probe to verify all three models respond to /api/generate with a tiny prompt under 5s, and confirm signal emissions resume non-zero confidence."
