# Kirk Advisory Source Routing

**Endpoint:** `GET /api/kirk/advisory?source=<value>`
**Handler:** `dashboard/app.py:13405-13501`
**Default source:** `paper`
**Audit:** HM-AU 2026-05-07 (this document)

## Behavior table

| `?source=` | Handler | Data source | Action logic | Returned positions |
|---|---|---|---|---|
| `paper` (default) | Engine path: `engine/kirk_advisory.py::generate_kirk_advisory()` | `data/real_holdings.json` via `_load_real_holdings()` | Full rule engine + `kirk_advisory_log` SQL writes + alert dedup + auto-dismiss for sold positions | All `is_active` accounts merged |
| `real` | Inline path: `dashboard/app.py:13422-13486` | `data/real_holdings.json` via `_read_real_positions_sync()` | Inline `TRIM/WATCH/HOLD` from regex-parsed `notes` (`pnl_pct%`); enriched dict with `action`, `message`, `pnl_pct`, `current_price` | Same 11 active-account positions but different field shape |
| `all` | Engine path called for envelope + side-effects; `real_positions` used for the `positions` field | `data/real_holdings.json` (read once, via the real path) | Engine runs (`kirk_advisory_log` writes, auto-dismiss); `paper_result["positions"] = real_positions` | Unique positions, no duplicates (post-HM-AU-β fix, commit `e6e9080` 2026-05-07) |
| (other) | Error fallback at line 13501 | n/a | n/a | `{"error": "unknown source: ..."}` |

## Source-vs-data naming contradiction

`?source=paper` reads `real_holdings.json` (Schwab + TradeStation), **not** Alpaca paper book.

This is **intentional** — commit `e41ddb2` "fix(Kirk): re-target advisor to Schwab real_holdings.json (Option A)" rerouted the advisory engine to Schwab as the truth source on 2026-05-05 per Admiral Option A decision. The source value stayed `paper` for back-compat with existing front-end callers; the underlying data flipped.

### Pre/post Option A semantics

| Era | `?source=paper` reads | `?source=real` reads |
|---|---|---|
| Before 2026-05-05 (`e41ddb2`) | Alpaca paper book (the "alpaca-mirror") | `real_holdings.json` (Schwab + TradeStation) |
| **After 2026-05-05** | `real_holdings.json` (engine path) | `real_holdings.json` (inline path) |

The post-Option A state means **both** named sources point at the same JSON file but go through different code paths with different output shapes:
- `paper` returns the engine-generated advisory (rule-engine action labels, market context, GEX regime, kirk_advisory_log persistence).
- `real` returns inline-parsed action labels (regex on `notes` string), with raw account info preserved.

## Snapshot data flow

```
Bonnie laptop                   bigmac
  PowerShell scp        →   ~/autonomous-trader/inbox/ (HM-AT-β)
                                       │
                                       ▼ launchd 60s poll
                              schwab_csv_watcher.sh
                                       │
                              import_schwab_csv.py     →   schwab_holdings (multi-snapshot DB table)
                                       │
                              sync_schwab_to_real_holdings.py
                                       │
                                       ▼
                              data/real_holdings.json  ← read by ALL Kirk advisory paths
```

The Kirk advisory endpoint reads `real_holdings.json`, not the `schwab_holdings` SQLite table directly. The sync script propagates the latest snapshot.

## Front-end callers (`dashboard/static/index.html`)

| Line | Call | Source value |
|---|---|---|
| 13196 | `fetch('/api/kirk/advisory?source=' + (typeof _kirkSource !== 'undefined' ? _kirkSource : 'real'))` | dynamic, defaults `'real'` if `_kirkSource` undef |
| 16772 | `fetch('/api/kirk/advisory')` | default `paper` |
| 24685 | `fetch('/api/kirk/advisory?source=' + _kirkSource)` | dynamic via `_kirkSource` |
| 25041 | `fetch('/api/kirk/advisory')` | default `paper` |
| 25101 | `fetch('/api/kirk/advisory')` | default `paper` |

Three of five callers use the engine path (default `paper`); two use `_kirkSource` (typically `real`). No front-end caller currently passes `?source=all`.

## Morning observation (2026-05-07) — explained

Reported: same `?source=paper` endpoint returned **23 positions at 06:50 MST** then **11 positions at 10:50 MST** after the Schwab CSV import.

Cause: between the two queries, `import_schwab_csv.py` + `sync_schwab_to_real_holdings.py` ran (per HM-AT-β backlog drain at ~09:14 MST), rewriting `data/real_holdings.json` from a stale Apr 30 snapshot (with 23 positions) to the current 2026-05-07 snapshot (11 positions). Both queries hit the engine path, both queries read `real_holdings.json` — the file content changed underneath them. **Not a routing inconsistency**; the snapshot itself shifted.

## Bugs flagged (history)

- **HM-AU-β** (surfaced by this audit, **fixed 2026-05-07 commit `e6e9080`**): `?source=all` previously returned duplicate positions because both the engine path and the inline real path read `data/real_holdings.json` post-Option A; the union concatenation produced each position twice. Fix: dropped `paper_positions` from the union; engine path still runs for its envelope (market_context, cash_recommendation) and side-effects (`kirk_advisory_log` writes, auto-dismiss for sold positions); `paper_result["positions"]` now contains `real_positions` only. Verified post-fix: 11 positions / 11 unique / dup count 0.

## Cross-references

- `e41ddb2` — Kirk Option A re-target to Schwab (2026-05-05)
- `796acbf` — HM-AJ-α/β/γ parse hardening + observability + alert hygiene
- HM-AT-β — `~/autonomous-trader/inbox/` migration (the pipe that feeds `real_holdings.json`)
- HM-AU — this audit (2026-05-07)
