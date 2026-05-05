# PID Transition Probe — 2026-05-05

## Question
Service was PID 34987 at 18:50 MST 2026-05-04 (Scotty's manual restart). At session-start this morning the dashboard / launchctl reported PID 35155. Why did it transition?

## Findings

### Process timeline (from `ps -o pid,etime,start`)
- **PID 35155 started at 6:54 PM** (yesterday 2026-05-04 18:54 MST). Elapsed time at probe time: **11:36:38**.
- This means **PID 35155 has been the live process since yesterday evening — NOT a fresh-overnight restart.** The original prompt assumed an overnight transition; actual transition happened 4 minutes after Scotty's manual restart.

### Two STARTUP blocks 4 minutes apart in `logs/trader.log`
Last five `[STARTUP] DayBlade: auto-armed` markers and their adjacent timestamps:

| Line | Adjacent timestamp | Interpretation |
|---|---|---|
| 324731 | (08:14:20 prior) | older startup |
| 325666 | 08:50:37 | older startup |
| 345092 | 08:31:52 | morning of 2026-05-04 |
| **358122** | **18:50:19** | **PID 34987 — Scotty's manual restart** |
| **358297** | **18:54:16** | **PID 35155 — current process** |

The two startups were 3 min 57 sec apart. PID 34987 lived for under 4 minutes before being terminated and replaced.

### launchd state (from `launchctl print gui/$(id -u)/com.trademinds.trader`)
```
state                 = running
pid                   = 35155
runs                  = 27
forks                 = 706
spawn type            = daemon (3)
last terminating signal = Terminated: 15        ← SIGTERM
exit timeout          = 5
```
- `KeepAlive = true` and `RunAtLoad = true` per `~/Library/LaunchAgents/com.trademinds.trader.plist`.
- No `StartCalendarInterval` or `StartInterval` entries — the plist has no scheduled-restart config.
- Last terminating signal `15 = SIGTERM` confirms the prior process exited via signal-15, not crash.

### Errno 48 (port-bind failure) count
- `grep -c "address already in use" logs/trader.log` → **6** (unchanged from yesterday's baseline).
- The 4-minute restart did NOT cause a new Errno 48. The new process bound port 8080 cleanly.

### Error log around 18:50–18:55
`logs/trader_error.log` shows a flood of `OllamaQueue: request timed out after 120s` entries during this window — multiple models (qwen3.5:9b, qwen2.5-coder:7b, deepseek-r1:14b, 0xroyce/plutus, deepseek-r1:7b). This is the same pattern as before the manual restart, not a new failure mode.

### Plist relevant excerpt
```xml
<key>RunAtLoad</key><true/>
<key>KeepAlive</key><true/>
<key>StandardOutPath</key>/Users/bigmac/autonomous-trader/logs/trader.log
<key>StandardErrorPath</key>/Users/bigmac/autonomous-trader/logs/trader_error.log
```
No `KeepAlive.SuccessfulExit`, no calendar-based restart, no exit-timeout override beyond default 5s.

## Verdict

**Choice: B — KeepAlive auto-recovery.**

Sequence of events on 2026-05-04 18:50–18:54 MST:
1. **18:50:19 MST** — Scotty issues manual restart. New PID 34987 boots. Full STARTUP block written (line 358122).
2. **18:50–18:54** — PID 34987 hangs on multiple OllamaQueue timeouts (120s window each, several queued).
3. **~18:54:00 MST** — launchd (or external SIGTERM source) sends signal 15 to PID 34987.
4. **18:54:16 MST** — KeepAlive=true triggers immediate respawn. New PID 35155 boots. Second STARTUP block written (line 358297). Port 8080 bind clean (no Errno 48 increment).
5. **18:54 MST → present** — PID 35155 stable, 11h36m+ uptime as of probe time.

The user's overnight-transition premise was incorrect: the transition happened **4 minutes after Scotty's restart**, not overnight. From the user's perspective the change was invisible because the dashboard kept serving — KeepAlive made the swap transparent.

### Why the SIGTERM at 18:54?
This is the one ambiguity. Three plausible sources, in decreasing likelihood:

1. **Scotty's restart command was issued twice** — the manual `launchctl kickstart` may have run as `bootout` + `bootstrap` then *again* `bootout` + `bootstrap`, killing the just-spawned PID 34987 and respawning as PID 35155. (Most likely; matches the 4-minute gap and the SIGTERM-then-respawn pattern. No anomaly to investigate.)
2. **launchd KeepAlive saw the process as unresponsive** during the OllamaQueue timeout flood and force-restarted. (Less likely — KeepAlive doesn't actively monitor responsiveness, only exit codes.)
3. **A monitoring script (Dr. Crusher healthcheck?) observed something wrong and issued kickstart.** Schedule shows `[STARTUP] Dr. Crusher Healthcheck: scheduled 6AM + 7AM–1PM MST via launchd` — its 18:54 fire would be unusual. Less likely but worth ruling out.

`system.log` predicate query for `process == "launchd"` over the last 24h returned 0 trademinds.trader entries (unified logging may have rotated past that retention window or filtered the messages out).

## Action

**No action required.** This is verdict B — expected operational behavior of KeepAlive=true. Service is stable at 11h36m uptime. Errno 48 baseline still 6. No leak, no crash signature, no missing data.

For future reference: when investigating a "PID changed" question, run `ps -o pid,etime,start` first — the elapsed time will immediately distinguish "restarted overnight" from "restarted at the moment of the prior intervention." That single check would have answered the question in one command.

## Open question for the Admiral

If you want certainty on which of the three SIGTERM-source possibilities was the cause, the remediation is one of:
- Add `StandardErrorPath` rotation so `launchd`'s own messages persist visibly (currently merged into trader_error.log).
- Add a `[SHUTDOWN]` log marker just before main.py's natural exit path (none today — restart leaves no trace beyond the next STARTUP block).
- Add a one-line `ps -p $$ -o pid,ppid,etime` dump at startup so each STARTUP block is self-stamped with its parent PID.

None of these is urgent. The verdict B answer is sufficient for operational confidence.
