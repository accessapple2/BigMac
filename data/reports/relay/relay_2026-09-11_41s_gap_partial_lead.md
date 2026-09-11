# Relay — partial lead on the "41-49s gap outside Ollama's own timing" item, 2026-09-11

The afternoon handoff doc (`relay_2026-09-11_afternoon_xo_directive_handoff.md`)
said this "ALSO" item was "not investigated — no prior trace exists anywhere
in the backlog." That's not quite right — a trace does exist, just on
olliemax's side, not in this repo's own backlog.

`~/modelworks/fleet_checks/ollama_churn/FINDINGS_FOR_SCOTTY.md` (read live via
`ssh olliemax` this session, dated 2026-09-11), "Timing, for your side"
section:

> In `arena_calls.jsonl`, Ollama's `total_duration` for the 16 calls is
> 2.2-10.4s, but `wall_s` is 5.4-52.0s. Three calls (17:10:51, 17:12:00,
> 17:13:16 UTC) spent 41-49s outside Ollama's reported time. That gap isn't
> visible from this box.

This is almost certainly the same three calls the XO directive's "ALSO" item
referred to (timestamps and the 41-49s figure match exactly). olliemax's own
conclusion: the gap is real (their `total_duration` accounts for generation
time only) but **not observable from their side** — meaning it's in the
network hop, connection setup/pooling, or client-side wait on bigmac's end,
not inside Ollama's own request handling.

**Not fully root-caused this session** — worth checking, in order:
1. `engine/providers/ollama_provider.py`'s connection-pool/retry logic around
   those three timestamps (2026-09-0x, need the exact date these calls are
   from — `arena_calls.jsonl`'s own metadata should have it).
2. Whether `_HM_OLLIE_STALE_SOCKET` handling (the same class of issue fixed
   2026-09-10) was in play, or whether this predates that fix.
3. DNS/Tailscale routing latency to `OLLAMA_URL` at those specific
   timestamps (`100.95.195.20`) — a stale ARP/route re-resolution could
   plausibly cost tens of seconds on a first reconnect.

Flagging as a real, partially-explained lead rather than a dead end — the
"not investigated, no trace exists" framing in the handoff doc undersold
what's actually available. Next session picking this up should start from
olliemax's FINDINGS_FOR_SCOTTY.md, not from scratch.
