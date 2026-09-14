# logging.md

> Relocated from CLAUDE.md by HM-PRIME Part C (move, not delete).

## Logging Sink Split (trader.log vs trader_error.log)

OllieTrades logs to two files with different sinks:

| File | Sink | What goes here |
|---|---|---|
| `logs/trader.log` | Rich `console.log(...)` calls | Per-cycle agent output, strategy ticks, market data, formatted user-facing log lines |
| `logs/trader_error.log` | Python `logger.info / .warning / .error` calls | Structured Python logging — including `engine.alert_channels` NTFY dispatch logs |

**Implication for investigations:** when checking whether a NTFY actually
fired, search `trader_error.log` for entries like:
- `[LRS] Alert dispatched [warning/{alert_type}]: {message}`
- `[LRS] ntfy sent [200]: ?? TradeMinds {Level}`

Searching only `trader.log` will miss NTFY firings — they POST at HTTP 200
and produce correctly; they just land in the other file.

## Daily rotation empties trader.log in place (05:00 MST)

`scripts/rotate_logs.sh` (cron `0 5 * * *`) gzips `trader.log` and
`trader_error.log` into `logs/_archive/<name>_YYYY-MM-DD.log.gz` once they are
over 100 MB — which `trader.log` is every day — then truncates the live file
in place (same inode, the trader's fd keeps writing). Anything logged once at
startup (every `[*-DAEMON] thread started` line) is gone from `trader.log`
the next morning. Search the archive too: `gzip -dc logs/_archive/trader_<date>.log.gz | grep ...`.

## Rich markup silently drops lowercase square-bracketed text (2026-09-14)

`trader.log` is written through Rich's `console.log`, which parses
`[something]` as a style tag when it starts with a lowercase letter, `#`,
`/` or `@`, and removes it from the output — no error. Measured against the
`.venv` Rich:

| Written | Appears in trader.log as |
|---|---|
| `McCoy screened scan [pre-open]: 16/100` | `McCoy screened scan : 16/100` |
| `slot [midday]`, `slot [pre_open]` | `slot `, `slot ` |
| `[MCCOY-DAEMON]`, `[AAPL]`, `[LATE — same-day recovery]` | unchanged (uppercase first char) |

So a search for `screened scan [pre-open]` never matches, in the live log or
the archives. **Fix for new code:** wrap interpolated values with
`rich.markup.escape(...)` (or write a literal `\[`). Done in
`engine/screened_scan_scheduler.py`. **Not swept:** ~24 other `console.log`
f-strings in `main.py`/`engine/` put an interpolated value inside brackets
(`Ready Room [{slot}]`, `CTO Advisory [{bt_label}]`, `Kirk Advisory [{slot}]`,
`Market scan triggered [{tier_label}]`, …) and lose it whenever the value
starts lowercase. When grepping for those lines, leave the bracketed part out.

## Screened-scan heartbeat (HM-SCREENED-SCAN-HB, 2026-09-14)

McCoy (`ollama-plutus`) and qwen3 (`ollama-qwen3`) screened-scan threads each
emit, every 60 s:

```
[SCREENED-HB] player=ollama-plutus tick=412 et=Mon 09:35 pre-open=fired midday=outside_window
```

Slot statuses: `fired`, `fired_late`, `fired_empty_screen`, `error:<Type>`
(all mark the slot done for the day), `done_today`, `outside_window`,
`skipped_seat:<reason>` (seat halted / no provider built — not marked done),
`deferred_next_tick`, `weekend`, `reset` (ET hour 0).

Rotation- and restart-proof copy: `data/screened_scan_heartbeat_<player_id>.json`
(`pid`, `thread_started_at_utc` stamped from inside the thread, `tick`,
`last_tick_at_utc`, per-slot `status` / `status_since_utc` /
`last_fired_at_utc` / `last_fired_status` / `n_symbols`). A `last_tick_at_utc`
more than ~2 minutes old while the trader is up means that thread is dead or
hung.
