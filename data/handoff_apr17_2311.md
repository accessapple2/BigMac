# Handoff Report — April 17, 2026 @ 23:11 AZ

## Mission Status: COMPLETE ✓

All five phases of the April 17 Pre-Market Hardening executed and verified.

---

## Phase 0 — Portfolio Data Freshness

| Item | Result |
|------|--------|
| Webull auto-sync plist | Loaded: 6:00 AM + 1:05 PM AZ daily |
| Metals sync plist | Loaded: 6:15 AM + 1:10 PM AZ daily |
| Metals positions (post-sync) | XAUUSD 1oz @ $4818.80 P&L –$431.19 / XAGUSD 45oz @ $79.14 P&L +$101.50 |
| Webull positions (current) | Stale 2026-03-28 → auto-sync fires 6:00 AM |

---

## Phase 1 — OOS Verdict Actions Applied (commit 82f86ba)

| Action | Code Location | Status |
|--------|--------------|--------|
| `covered_call` disabled | `engine/crew_specialization.py` line 103 | ✓ LIVE |
| `covered_call` removed from McCoy (ollama-plutus) | `engine/crew_specialization.py` line 121 | ✓ LIVE |
| `rsi_bounce` Gate 7b — CAUTIOUS/MIXED only | `engine/crew_scanner.py` lines 3041–3052 | ✓ LIVE |
| `RSI_BOUNCE_ALLOWED_REGIMES = {"CAUTIOUS", "MIXED"}` | `engine/crew_scanner.py` line 195 | ✓ LIVE |

Basis: OOS-A Sharpe –0.556 (covered_call, 2022 bear); rsi_bounce BEAR –6.6 / CRISIS –12.9.

---

## Phase 2 — Safe Cleanup (commit 76ad811)

| Item | Result |
|------|--------|
| `~/sync_portfolio.py` | Archived → `~/bigmac_migration_retired/20260416/` |
| `~/trader.py` | Archived |
| `~/trades.db` | Archived |
| `~/paper-trader/server.log` | Truncated 181MB → 0B (tail saved 352KB) |
| `~/start-trademinds.sh` | **PENDING** Admiral OK — stale ngrok+nohup launcher |
| `~/cloudflared.log` | Kept — active until G1 cloudflared reconfigured |
| Space reclaimed | ~181MB |

---

## Phase 3 — Controlled Restart

| Service | Old PID | New PID | Port | Status |
|---------|---------|---------|------|--------|
| com.trademinds.trader | 5383 | 7056 | 8080 | ✓ UP |
| com.trademinds.signal-center | 59607 | 7085 | 9000 | ✓ UP |
| Tractor Beam | — | — | 9100 | Not running (was not running pre-restart) |

Restart order: signal-center → trader (graceful unload/load).

---

## Phase 4 — Live-Readiness Verification

### Service Health
| Endpoint | Result |
|----------|--------|
| `GET /api/status` | `status=running, season=6, players=32, trades=91` |
| Signal Center :9000 | Responding (auth redirect) |
| Tunnel PID 73732 | Running |
| Ollama :11434 | Healthy |

### Required Models
| Model | Status |
|-------|--------|
| `0xroyce/plutus` (McCoy) | ✓ Present |
| `qwen3.5:9b` (primary) | ✓ Present |
| `deepseek-r1:14b` (Spock) | ✓ Present (14b, was 7b — check crew config) |
| `mistral:7b` (Pike) | ✓ Present |
| `gemma3:4b` (Picard/Scotty) | ✓ Present |
| `llama3.1` (Uhura) | ✓ Present |
| `qwen3:8b` (Dax/Kirk/Sarek) | ⚠ NOT FOUND — `qwen3:14b` available instead |

### Launchd Plist Inventory (22 total)
```
RUNNING:
  7056  com.trademinds.trader          (8080)
  7085  com.trademinds.signal-center   (9000)
  73732 com.trademinds.tunnel
  876   com.trademinds.scanner
  866   com.trademinds.watchdog
  861   com.trademinds.mcp

SCHEDULED (fire on interval):
  com.trademinds.webull-sync     (NEW — 6:00 AM + 1:05 PM)
  com.trademinds.metals-sync     (NEW — 6:15 AM + 1:10 PM)
  com.trademinds.premarket
  com.trademinds.caffeinate
  com.trademinds.healthcheck
  com.trademinds.crew
  com.ollietrades.riker-synthesis
  com.ollietrades.ghost-trader
  com.ollietrades.uhura
  com.ollietrades.etfregime
  com.ollietrades.morningbriefing
  com.ollietrades.nightly-backtest  ⚠ BROKEN (import path bug)
  com.ollietrades.danelfin-update
  com.ollietrades.archer-briefing
  com.ollietrades.optionsflow (exit=1, needs investigation)
```

### OOS Verdict Actions Confirmed Live
- `covered_call`: disabled in `ALLOWED_STRATEGIES` ✓
- `rsi_bounce`: Gate 7b blocks BULL/BEAR/CRISIS/VOLATILE regimes ✓

### Captain's Portfolio (portfolio_id=5)
```
XAUUSD: 1.0 oz, price=$4818.80, P&L=–$431.19  updated=2026-04-17 05:59:56
XAGUSD: 45.0 oz, price=$79.14,  P&L=+$101.50  updated=2026-04-17 05:59:56
```
Net metals P&L: –$329.69

### Signal Flow
Last signals: 2026-04-14 15:56 (market closed — normal)

---

## OOS Validation Summary (all three windows)

| Candidate | Window | SPY | Sharpe | WR | Trades |
|-----------|--------|-----|--------|----|--------|
| IS (in-sample) | 180d | +17.5% | 4.845 | 100% | — | OVERFIT |
| OOS-A | 2024 bull | +23.0% | 2.692 | 65.8% | 456 | ✓ |
| OOS-C | 2022 bear | –24.3% | 2.087 | — | 890 | ✓ |

### CSP (core strategy)
- OOS-A Sharpe: **+6.05** (BULL/CAUTIOUS dominant)
- OOS-C Sharpe: **+5.42** (bear regime, 89.9% WR)
- Verdict: **regime-robust** → PRIMARY STRATEGY

---

## Pending (Admiral Decisions Required)

1. **`~/start-trademinds.sh`** — Archive? (stale ngrok+nohup, superseded by launchd+cloudflared)
2. **`com.ollietrades.optionsflow`** — exit=1; needs investigation
3. **`com.ollietrades.nightly-backtest`** — broken import (`engine.super_backtest_v5` fails as script).
   Options: (a) fix plist to use `-m engine.nightly_backtest`, (b) add `sys.path` fix to script, (c) retire
4. **`qwen3:8b` missing** — only `qwen3:14b` available. Affects Dax, Kirk, Sarek crew config.
   Options: `ollama pull qwen3:8b` or update crew.json to use `qwen3:14b`

---

## Commits This Session

| Hash | Phase | Description |
|------|-------|-------------|
| `82f86ba` | Phase 1 | OOS verdict actions — covered_call disabled, rsi_bounce gate |
| `76ad811` | Phase 2 | Cleanup manifest + portfolio hardening |

**Main branch is 7 commits ahead of origin/main.**
Admiral runs push manually (VPN off required).
