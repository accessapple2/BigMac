# Relay — 2026-08-28 (late) — 429 remediation Parts A + D, trader restart

## Context

Follow-on to the same-evening Signal Center / Kirk briefing relay
(`relay_2026-08-28_signal-center-and-kirk-briefings.md`). Separate directive:
"429 REMEDIATION v2" — four independent services throwing "Too Many
Requests" (Yahoo/yfinance, Alpaca, Polygon, ntfy), ordered A (Yahoo) → D
(ntfy) → C (Polygon) → B (Alpaca), weekend execution preferred, explicit
scope fence excluding the gpusweep/woot GPU-watch stack (separate project,
its own Reddit 429s already handled there).

**This pass covers A and D only, restart included.** C is investigation-only
per its own explicit Admiral gate (not built). B not started.

## Verify-before-fix: several of the directive's specifics didn't match live state

Per standing doctrine (and simple prudence for a live-trading box), checked
everything against real logs/code rather than trusting the directive's
numbers:

- **The named 21:10/22:10 "Yahoo chain fetch" storm is not Yahoo at all.**
  Every line in both windows is `engine/long_range_sensors.py::send_ntfy()`
  failing (429 / no-route-to-host / timeout) — zero genuine yfinance 429s
  anywhere in `trader_error.log`. That function already carries its own
  2026-07-10 comment tracing this exact pattern.
- **That storm is also very likely historical, not live.** `trader_error.log`
  is 101MB/837k lines and has never been rotated by timestamp in a way that
  lets a bare `grep "21:1[0-9]"` distinguish today from six weeks ago.
  `engine/alert_channels.py::_send_ntfy()` has had a hard `return False`
  "DECOM-SILENCE" guard (dated 2026-07-19 in-comment) sitting uncommitted in
  the working tree — which structurally means LRS's alerts (they route
  through this same function even with `bypass_rate_limit=True`) can't
  produce a real ntfy 429 once that guard is loaded. The storm's real
  attributable count (~13,300 in one week) is independently documented in
  that file's own 2026-07-10 docstring, i.e. before DECOM-SILENCE existed.
- **"233-symbol" Alpaca snapshot request** — found an 829-symbol one instead
  (Part B, not started this pass — flagging the discrepancy for whoever
  picks up B).
- **The three named ntfy culprits weren't the real one.**
  `watchdog_supervisor.sh` sends no ntfy at all (just relaunches
  `watchdog.py`). `scripts/hm_ops_sentinel.py` doesn't exist (renamed away in
  the 2026-07-22 stand-down) — its cron line was erroring "file not found"
  every 5 min for weeks, never sending anything, so it wasn't contributing
  429s either. `HM-TRADER-KEEPALIVE`'s curl is already event-gated (only
  fires when `main.py` is actually found dead). The real still-live,
  unprotected ntfy sender turned out to be `watchdog.py::push_alert()` — not
  named in the directive, found by tracing actual call sites.

None of this is a knock on the directive — just what "verify before fix"
turned up. Everything below is grounded in what's actually true tonight.

## Shipped (commit `87e88c5`, pushed to `exec-pipeline`)

**Part A (Yahoo):**
- `engine/yf_safe.py` — new `yf_call_safe()` / `reset_sweep()` /
  `YFSweepAbort`: paces any two calls through it to ≥2s apart, retries a
  rate-limit-shaped exception with 2s/4s/8s backoff, raises `YFSweepAbort`
  after 5 consecutive failures (added alongside, not replacing, the
  existing HM-BL delisted-ticker cache in the same file).
- `engine/alpha_signals.py` — `run_earnings_signals()` (24-symbol
  `yfinance.Ticker` loop: `.calendar` + `.earnings_history`) and
  `run_vix_structure()` (2 raw Yahoo chart-API calls) now route their real
  network hits through `yf_call_safe()`, breaking their loop on
  `YFSweepAbort` instead of grinding through the rest doomed to fail too.
- `uoa/scraper.py` — the actual "chain fetch" the directive means: the
  528-symbol UOA scanner's `stock.option_chain(exp_date)` calls, previously
  completely unpaced *within* a ticker (only a weak 1-symbol-per-10-ticker
  sleep existed at the outer level). Now paced/backed-off; `YFSweepAbort`
  propagates out of `_scan_single_ticker` to stop the whole scan cleanly.
  **Runtime cost worth watching**: ≥2s/chain-fetch will meaningfully
  lengthen this scan for symbols with multiple relevant expirations — first
  live run is the thing to check, not just "did it error."
- Zero real yfinance 429s existed at implementation time — this is
  precautionary hardening for ~70 direct-yfinance-import call sites
  repo-wide, not a reactive fix. Only the two genuinely `option_chain`/loop
  call sites got migrated; a full 70-file sweep was judged out of realistic
  scope for one pass.

**Part D (ntfy):**
- Committed the already-written-but-uncommitted `alert_channels.py` change:
  DECOM-SILENCE (blanket ntfy silence, 2026-07-19) + PUSHOVER-RED-ALERT
  (RED_ALERT still reaches Pushover, 2026-08-28) — not authored this
  session, verified coherent, now live (see restart below).
- `watchdog.py::push_alert()` — the one genuinely still-live, unprotected
  ntfy pathway (deliberately dependency-free from `engine/`, so it needed
  its own self-contained fix, not a shared import): identical title+body
  suppressed 30 min, daily hard cap 200, 429 backoff 5s/20s/60s. No 429s
  observed from this specific sender — precautionary, same as Part A.
- Crontab (backed up before each change, `~/backups/cron/`): removed the
  expired HM-FORGE-PAGER oneshot; commented out (not deleted —
  restore-alongside-the-standdown-decision, per
  `QUESTION_fleet-standdown-reversal.md`) the dead `hm_ops_sentinel.py` and
  `iren_flip_watch.py` lines; fixed `HM-TRADER-KEEPALIVE` to invoke
  `trader_restart.sh` via `/bin/zsh` instead of `/bin/bash` — the script is
  zsh-shebang'd and CLAUDE.md explicitly documents "do NOT prepend bash"
  for this exact script.

## The gap that mattered (same shape as the 2026-07-18 season-rotation one)

`alert_channels.py`'s last edit (20:26:30) postdated the then-running
trader's own start (20:08:29) — the fix was on disk and even (per the
in-process log evidence) partially behaving as if active, but was never
confirmed genuinely loaded, and the new Part A/D code hadn't been written
yet regardless. Per explicit instruction, did NOT restart blind:

1. Fixed `trader_restart.sh`'s invocation shell first (see crontab above).
2. Committed the full changeset (`87e88c5`).
3. **Live-tested `_send_pushover()` directly** (isolated from
   `send_alert()`'s DB-write/email side effects) via the trader's own
   `.venv/bin/python3` — returned `True`. **Admiral confirmed receipt on
   phone before anything further happened.**
4. Only then: `zsh scripts/trader_restart.sh` — market closed, doctrine
   pre-authorizes (same authority cited in the 07-18 precedent).

## Live-verification results (post-restart)

- `trader_restart.sh` output: old PID 441 killed, WAL checkpointed, single
  writer, new PID 22370 bound `:8080` — "RESTART OK — orphan-free."
- `curl http://127.0.0.1:8080/api/health` → `server_up:true`,
  `scheduler_errors:0`.
- `trader_error.log` post-restart: zero tracebacks/ImportError/
  ModuleNotFoundError/SyntaxError/AttributeError.
- `trader.log`: normal startup sequence (event tape armed, Alpaca paper
  bridge initialized, price cache warmed, endpoints responding 200).
- `from engine.yf_safe import yf_call_safe, YFSweepAbort, reset_sweep`,
  `from engine import alpha_signals`, `from uoa.scraper import UOAScraper`,
  `from engine.alert_channels import _send_ntfy, _send_pushover` — all
  import cleanly under `.venv/bin/python3`, the exact interpreter+file PID
  22370 read at its own startup 42 seconds prior. Didn't wait for a natural
  in-process alert to fire as further proof — a fresh process reading a
  file that imports cleanly *is* the proof; there's no partial-load state
  in Python to worry about here.

## NOT done this pass

- **Part C (Polygon)** — not built. Its own directive text explicitly gates
  code going live on an Admiral review of which of 12 modules feed
  BENCH-gated execution (staleness-tolerance list). That investigation
  hasn't started; will follow the same Question Relay process before any
  limiter goes live on a market day.
- **Part B (Alpaca)** — not started. Note the 829-symbol vs. directive's
  "233-symbol" discrepancy above for whoever picks this up.
- **`watchdog.py` itself is not yet restarted** — its hardening is committed
  and correct on disk, but (same gap-that-mattered logic as above) it's a
  separate resident process from `main.py` and wasn't restarted this pass.
  The Admiral's instruction was specifically "restart the trader"; didn't
  extend that to watchdog.py without being asked. Flagging so this doesn't
  silently look "done" — it isn't live yet either.
- **`uoa/scraper.py`'s `fast_info`/`history`/`options` per-ticker calls**
  (as opposed to the per-expiration `option_chain()` calls) remain unpaced —
  lower volume (once per ticker, not once per expiration) but still real
  unwrapped yfinance calls. Residual gap, not fixed this pass.
