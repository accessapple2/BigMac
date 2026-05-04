# Sprint backup archive — April 2026

99 `.bak` files originally scattered across the repo as `<file>.bak.<sprint>_<timestamp>`
or `<file>.bak.<sprint-name>` with April mtimes.

Archived 2026-05-04 as part of HM-H after HM-G's gitignore expansion (`*.bak.*`
patterns now caught for new files; this archive captured the pre-existing tracked ones).

## Filename convention

Slashes in original paths are flattened to double underscores (`__`). Examples:
- `dashboard__app.py.bak.20260426`           → was `dashboard/app.py.bak.20260426`
- `dashboard__static__index.html.bak.20260426.troi` → was `dashboard/static/index.html.bak.20260426.troi`
- `engine__providers__grok_provider.py.bak.20260426` → was `engine/providers/grok_provider.py.bak.20260426`

## Bucketing rule

Filename-encoded date (`.bak.YYYYMMDD*`) takes precedence. When filename has no
date encoded, the file's mtime determines the bucket. April-mtime files with
sprint-named suffixes (e.g., `webull_to_schwab`, `kirk_real_only`, `ars_real_only`)
landed here.

## Source sprints

These archives correspond to the late-April 2026 sprint cycles:

- 2026-04-26: Sunday Drydock — see `docs/SUNDAY_DRYDOCK_2026-04-26_FINAL.md`
- 2026-04-27: Monday Sprint — see `docs/MONDAY_SPRINT_2026-04-27.md`
- 2026-04-28: kirkphase / battlestation / tooltip work
- 2026-04-30: routingleak fix + S6 sim backtest iterations

## To restore

`cp` the archived file back to its original path (decode the `__` → `/`):
```bash
cp archive/sprint-backups/2026-04/dashboard__app.py.bak.20260426 dashboard/app.py.bak.20260426
```

Note: per `.gitignore` `*.bak.*` patterns added in HM-G (commit `f7181f0`),
restored files would be ignored from new tracking unless force-added.
