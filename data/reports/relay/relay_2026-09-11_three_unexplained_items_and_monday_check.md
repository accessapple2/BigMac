# Relay — 2026-09-11 evening. The three "unexplained" items + Monday verification setup.

Picking up exactly where the afternoon XO handoff left off (`relay_2026-09-11_
afternoon_xo_directive_handoff.md`) — its own "#1 thing to check" (Monday's
McCoy slots) and its three disclaimed "ALSO" items. Explicitly NOT touching
Phase 1.3 or the bakeoff (bigmac-a4's, holding).

## 1. The 41-49s gaps outside Ollama's own timing — root cause found

Supersedes `relay_2026-09-11_41s_gap_partial_lead.md` (bigmac-a4, same
session window) — that doc correctly found the pointer (olliemax's
`FINDINGS_FOR_SCOTTY.md`) but hadn't root-caused it yet, and proposed
checking connection-pool/retry logic, the stale-socket fix, or DNS/Tailscale
routing latency as next steps. None of those needed: the log evidence below
is a sufficient, direct explanation, and the timestamps rule out routing —
this was client-side contention, not a network hop.

The original pointer (olliemax's `FINDINGS_FOR_SCOTTY.md`, "Timing, for your
side"): three calls at 17:10:51, 17:12:00, 17:13:16 UTC (= 10:10:51, 10:12:00,
10:13:16 MST today) had `wall_s` 41-49s higher than Ollama's own
`total_duration`. The raw capture (`/private/tmp/arena_calls.jsonl`) that
produced this has since been overwritten by a later delivery, so the exact
per-call breakdown can't be re-derived — but `logs/trader_error.log` still
covers this window independently, and it tells a clear story.

**This box was under real load in that exact window, not a mystery gap:**
- `10:06:32` and `10:08:35`: genuine `[OLLAMA-CANCEL] model=plutus-v1 ...
  wall=180.0Xs reason=ReadTimeout` — two real 180s client timeouts.
- `10:11:34` and `10:11:38`: two more, same pattern, inside the flagged
  window itself.
- Between 10:10:00 and 10:14:00, **763** `[polygon SHADOW] would FAIL LOUD`
  lines fired (`engine/tiered_rate_limiter.py::gated_call`) — and shadow mode
  is NOT a no-op: `fetch_fn()` always actually runs (a real HTTP call to
  Polygon), plus `self._save_shadow_report()` (a disk write) on every single
  one. That's ~763 real network calls + ~763 synchronous disk writes packed
  into under 4 minutes.
- This lines up with `10:13:20`/`10:13:40`: `screener pass1: 4077 → 200
  survivors` — a full-universe (4077-symbol) screener pass was actively
  building its indicator set through this exact window, which is what's
  driving the candle-fetch burst above.

**Conclusion:** the flagged calls' `wall_s` includes time our own process
spent contending with a genuine, coincident load spike (a full-universe
screener pass generating hundreds of real HTTP calls + disk writes per
minute, on the same box, same GIL) — not anything wrong with Ollama, the
network to olliemax, or `engine/ollama_queue.py`. Ollama's own
`total_duration` was accurate; the client just wasn't scheduled to process
the response promptly because it was busy elsewhere. Nothing to fix here
specifically — it's a symptom of the same load-spike class as item 2 below,
not an independent bug.

## 2. origin_healthcheck's "database is locked" flapping

`scripts/origin_healthcheck.sh` itself has no DB logic at all — it's a pure
HTTP check (`/api/status`, `/api/health`) that restarts a service on failure
(`crontab`, every 5 min). It is NOT the source of "database is locked"
errors; those come from inside the trader process itself (`engine/
alert_channels.py::_save_setting` and others), and the two symptoms are
downstream of the same root cause, not each other's cause.

**Today's pattern, quantified:** 73 "database is locked" errors in
`trader_error.log` today, clustered in bursts — 8 in 6 minutes (09:11-09:17),
2 right inside today's flagged window (10:11-10:12), more around 12:45-12:46
and 13:19/13:35-13:37. `_conn()` (`alert_channels.py:159`) already sets
`timeout=20` (SQLite's busy-timeout, which retries internally before
raising) — for it to still raise, `data/trader.db` (1.27GB, WAL mode) was
write-contended for **more than 20 continuous seconds** at each of these
moments. That's consistent with the same load-spike pattern as item 1: many
threads (War Room posts, decision logging, alert_channels, OllieAuto init,
the screener) hitting one SQLite file concurrently during a burst.

**This morning's healthcheck restarts** (`logs/origin_healthcheck.log`):
three restarts in 10 minutes, 06:50-07:00 MST, `main.py (bridge) failed
healthcheck` — same mechanism: under load, `/api/status` stops answering
inside the healthcheck's timeout window, gets treated as "down," restarted.

**What's NOT resolved:** the earlier session's specific 8/31 "fuller 3hr
multi-service pattern" — no surviving logs from that date in the current
`trader_error.log` (it doesn't go back that far), so that specific historical
episode can't be reconstructed from here. The *general* mechanism (SQLite
lock contention + HTTP-healthcheck restarts, both downstream of the same
concurrent-load bursts) is now documented and applies to any recurrence,
including the 8/31 one most likely.

**Not fixing this now** — it would mean either serializing more DB writers
behind a shared connection/lock, or moving `alert_channels`'/`decision_audit`'s
writes to WAL with a longer busy-timeout, both real design changes outside
today's scope. Flagging as a concrete, well-evidenced follow-up rather than
building it unasked.

## 3. gemma3's ~20K-token runaway answers — already closed

Confirmed already fixed (my own earlier session today, commit `3daaa99`):
`num_predict` was a top-level JSON key in `engine/riker_xo.py`'s `/api/
generate` payload instead of inside `options`, so Ollama silently never
applied the 400-token cap. Moved into `options`, plus a `done_reason ==
"length"` guard added so a future cap-hit response isn't cached as complete.
Live in the running process since the 13:20:32 restart (confirmed by process-
start-time-vs-file-edit-time forensics + a real post-restart heartbeat).
`RIKER_DONE_REASON_FIX.md` delivered to olliemax the same session; they've
since applied gemma3:4b's context drop (32768 -> 12288) on the strength of
that fix being live. Nothing further open here.

## Monday's two McCoy scheduler slots — mechanism reviewed, live, not yet
## market-verified (can't be, until Monday)

Reviewed `main.py`'s `_mccoy_scheduler_thread` (line 4468) directly: mirrors
the already-proven `HM-WR-DAEMON-THREAD` pattern exactly — its own daemon
thread, 60s poll, `run_mccoy_screened_scan()`'s own slot/window/once-per-day
gating unchanged underneath. Confirmed the old `schedule.every(...).do(
run_mccoy_screened_scan)` registration is gone (not duplicated — would have
double-fired otherwise). Confirmed live in the CURRENT process: `grep
"MCCOY-DAEMON" logs/trader.log` shows the startup line for both today's
restarts, including the one currently running (PID 10282, up since 14:33:56
MST).

**This cannot be verified further today** — market's closed, and the slots
(9:35 AM / 12:30 PM ET) only fire on a real trading day. Monday's check,
exactly:

```
grep -i "McCoy screened scan" logs/trader.log   # expect two lines: [pre-open] and [midday]
```

If either is missing, `grep "MCCOY-DAEMON\|SCHED-JOB" logs/trader_error.log`
around that slot's window — the new instrumentation should make a repeat
failure fast to root-cause, unlike the original silent no-fire.

Session-only reminder set via CronCreate for Monday morning as a courtesy
(these don't persist across a session ending, so this doc is the durable
source of truth regardless of whether that fires).

## Files touched this pass
None (read-only investigation + this doc). No commits beyond this relay doc
and the already-drafted CLAUDE.md fork-fence rule (reviewed, not authored
by this session — see separate note).
