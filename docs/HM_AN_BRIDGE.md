# HM-AN Bridge — Signal Center → Dashboard

**Shipped:** 2026-05-10 (Phase 1 of Dashboard Remodel v1)
**Status:** read-only foundation
**Commits:**
- `c4223fd` — feat(momentum): HM-AN Phase 1.1 — bridge module skeleton
- `2a0b58d` — feat(dashboard): HM-AN Phase 1.2 — `/api/momentum/heartbeat` + `/api/momentum/recent_signals`
- *(this commit)* — docs(hm-an): bridge documentation for Phase 1 endpoints

## Endpoints

| Method | Path | Returns |
|--------|------|---------|
| GET | `/api/momentum/heartbeat` | bridge + Signal Center liveness probe |
| GET | `/api/momentum/recent_signals?since_minutes=60&limit=100` | recent signals feed |

### `/api/momentum/heartbeat`

```json
{
  "bridge_alive": true,
  "signal_center": {
    "reachable": true,
    "endpoint": "/api/health",
    "last_check_ts": "2026-05-11T01:39:11.973318Z",
    "error": null
  },
  "phase": "HM-AN.1"
}
```

If Signal Center is unreachable, `reachable=false` and `error` carries the most recent request exception string. The probe tries `/api/health` first (the only verified-200 endpoint as of 2026-05-10), then falls back to `/health`, `/api/status`, `/status`, `/`. `allow_redirects=False` ensures the Flask auth wall (302 → `/login`) is never misread as healthy.

### `/api/momentum/recent_signals`

Query params:
- `since_minutes` — window in minutes, bounded to **[1, 1440]** (24h max)
- `limit` — max rows, bounded to **[1, 500]**

Response shape:
```json
{
  "count": 12,
  "since_minutes": 60,
  "signals": [
    {
      "id": 12345,
      "player_id": "energy-arnold",
      "symbol": "QQQ",
      "signal": "BUY",
      "confidence": 0.82,
      "created_at": "2026-05-10T13:45:00",
      "reasoning": "..."
    }
  ]
}
```

LEGACY_BIMODAL March 23–25 rows are filtered out automatically (`reasoning NOT LIKE '%[LEGACY_BIMODAL%'`).

## Module layout

| Path | Purpose |
|------|---------|
| `engine/momentum/__init__.py` | package marker |
| `engine/momentum/bridge.py` | `BridgeHealth`, `check_signal_center_health()`, `fetch_recent_signals()` |
| `dashboard/app.py` (EOF, between `=== HM-AN Phase 1 ===` anchors) | two FastAPI route handlers |

## Phase 1 design decisions

1. **Data source: `data/trader.db.signals` (direct)** — not the Signal Center HTTP API. The Phase 1 brief explicitly scoped this as "trader.db read path is the source of truth" with a swap point in Phase 2+.
2. **`allow_redirects=False`** on the health probe — Signal Center's `/`, `/health`, `/status`, `/heartbeat`, `/signals` all return 302 to its `/login` (Flask auth wall). The dashboard process is local-host on bigmac and can reach `/api/*` paths without auth.
3. **Schema:** `signals.signal` is the column name (not `side` — original directive stub said `side` but discovery confirmed `signal`).
4. **Idempotency:** `dashboard/app.py` insertion is wrapped in greppable anchor comments so re-running the directive can detect and skip the apply step.

## Phase 2+ extensions (not yet shipped)

- `/api/momentum/race` — top gainers since open (Race tile, Phase 2)
- `/api/momentum/scanner` — 5-min movers + RVOL (Scanner tile, Phase 3)
- `/api/momentum/detail/{ticker}` — multi-timeframe + flags (Detail panel, Phase 4)
- **Data-source swap candidate:** `fetch_recent_signals` → Signal Center `/api/signals`. That endpoint returns richer payload (`entry_price`, `stop_loss`, `take_profit`, `agent_name`, `sources_json`, `model_used`, `timeframe`) which Race/Scanner tiles can render directly without re-deriving prices from `trader.db`.

## Restart required to activate Phase 1.2 endpoints

```bash
launchctl kickstart -k gui/$(id -u)/com.trademinds.trader
```

(Admiral handles after push. Do not restart automatically — the running service is currently serving live dashboard traffic.)

## Smoke test (post-restart)

```bash
# Heartbeat
curl -s http://localhost:8080/api/momentum/heartbeat | jq

# Recent signals (last 30 min, max 10)
curl -s "http://localhost:8080/api/momentum/recent_signals?since_minutes=30&limit=10" | jq '.count'

# Input bounds (should clamp to 1440 and 500)
curl -s "http://localhost:8080/api/momentum/recent_signals?since_minutes=99999&limit=99999" | jq '.since_minutes'
```

## Cross-references

- `data/scotty_proposals/hm_an_scope.md` — prior scope doc (Scotty 2.0, filed 2026-05-10 morning)
- `docs/SCOTTY_AUDIT_2.md` §I — original "wire dashboard reads into Signal Center" recommendation
- `data/scotty_phase1_discovery.md` — Phase 0 discovery summary
- `CLAUDE.md` "Network Bindings" — HM-AW tracks reopening Signal Center to LAN (currently 127.0.0.1)
