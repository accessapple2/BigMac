# Sprint backup archive — May 2026

10 `.bak` files with May 2026 mtimes (sprint-named, no date in filename).

Archived 2026-05-04 as part of HM-H. These are pre-fix snapshots of files that
have shipped multiple times since — bull-call-spread v1 (`bcs_v1`), bear-put-spread
v1 (`bps_v1`), and strategies crisis-fix work that landed early May.

## Filename convention

Slashes in original paths flattened to double underscores (`__`). Sprint names
preserved verbatim:
- `main.py.bak.bcs_v1`       — main.py snapshot before bull-call-spread v1 ship
- `main.py.bak.bps_v1`       — main.py snapshot before bear-put-spread v1 ship
- `engine__strategies.py.bak.crisis_fix` — engine/strategies.py before crisis-fix
- etc.

## Source sprints

- `bcs_v1` / `bps_v1`: bull-call-spread + bear-put-spread strategy modules.
  Today's gate-flip commit `df7320c` is the current execution state of these
  strategies; these `.bak.*_v1` files are pre-fix snapshots.
- `crisis_fix` / `crisis_excl`: `engine/strategies.py` crisis-mode handling
  iteration (early May).

## To restore

`cp` the archived file back to its original path (decode the `__` → `/`):
```bash
cp archive/sprint-backups/2026-05/main.py.bak.bcs_v1 main.py.bak.bcs_v1
```

Note: per `.gitignore` `*.bak.*` patterns added in HM-G (commit `f7181f0`),
restored files would be ignored from new tracking unless force-added.
