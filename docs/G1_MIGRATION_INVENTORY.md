# G1 Migration Inventory — OllieTrades Season 6
**Generated:** 2026-04-16
**Purpose:** Complete inventory for migrating USS TradeMinds from macOS BigMac (arm64) to Ubuntu G1 server.
**Status:** READ-ONLY audit — no modifications made.

---

## 1. System Baseline

| Item | Value |
|------|-------|
| Hostname | bigmac |
| OS | macOS Sequoia 26.4.1 (Darwin 25.4.0) |
| Architecture | arm64 (Apple Silicon) |
| Shell | zsh |
| Home | /Users/bigmac |
| Project root | /Users/bigmac/autonomous-trader |
| System Python | 3.14.3 (Homebrew `/opt/homebrew/bin/python3`) |
| Primary venv Python | 3.9.6 (`venv/bin/python3`) |

**G1 Target OS:** Ubuntu (x86_64 or arm64 — confirm before migration)
**Key arch risk:** arm64 wheels for numba, llvmlite, pyobjc-* will NOT exist on x86_64. Numba/vectorbt may need rebuild.

---

## 2. Python Environments

### 2.1 `venv` — PRIMARY/PRODUCTION (Python 3.9.6)

Used by: main.py, most engine scripts, all com.trademinds.* plists (except crew).

**~200+ packages. Key dependencies:**

```
alpaca-py==0.43.2
anthropic==0.84.0
APScheduler==3.11.2
beautifulsoup4==4.14.3
ccxt==4.5.40
crewai==0.5.0
cryptography==46.0.5
edgartools==4.6.3
fastapi==0.115.14
Flask==3.1.3
groq==1.0.0
langchain==0.1.0
langgraph==0.6.11
matplotlib==3.9.4
numba==0.56.4          ← arm64 wheel, may need rebuild on x86_64
numpy==1.23.5
ollama==0.6.1
openai==2.24.0
openbb==4.4.2          ← large dep tree, verify on Ubuntu
pandas==2.3.3
playwright==1.58.0
pydantic==2.12.5
pyobjc-core==12.0      ← macOS ONLY — remove on G1
pyobjc-framework-Cocoa==12.0  ← macOS ONLY — remove on G1
pyotp==2.9.0
rich==14.3.3
scrapling==0.2.99
SQLAlchemy==2.0.48
uvicorn==0.34.3
vectorbt==0.28.2       ← depends on numba, arm64 risk
webull==0.6.1
yfinance==0.2.66
```

Full freeze: `venv/bin/pip freeze > requirements_venv.txt`

### 2.2 `.venv` — Newer stack (Python 3.14.3, 79 packages)

Used by: unclear — newer experiments. Check what imports from here before migrating.

### 2.3 `.venv-crew` — CrewAI (Python 3.12.13, 261 packages)

Used by: `main_crew.py` via `com.trademinds.crew.plist`
Interpreter path in plist: `.venv-crew/bin/python`

### 2.4 `scrapling-venv` — Web scraping (Python 3.14.3, 69 packages)

Used by: scrapling-related scripts. Isolated to avoid conflicts with 3.9.6 stack.

**G1 Action:** Recreate all 4 venvs. pyobjc packages are macOS-only — remove from requirements on G1.

---

## 3. Ollama Models

**Total local storage: ~57 GB** (`.ollama/models` dir confirmed 57GB)
**Ollama version:** current (check `ollama --version` before migration)

| Model | ID | Size | Last Used | Role |
|-------|----|------|-----------|------|
| llama3.2:3b | a80c4f17acd5 | 2.0 GB | 6 days ago | Fast tasks |
| phi4:14b | ac896e5b8b34 | 9.1 GB | 6 days ago | Analysis |
| deepseek-r1:14b | c333b7232bdb | 9.0 GB | 6 days ago | Reasoning |
| qwen3:14b | bdbd181c33f2 | 9.3 GB | 6 days ago | Primary reasoning |
| mistral:7b | 6577803aa9a0 | 4.4 GB | 8 days ago | Emergency fallback |
| hf.co/0xroyce/Plutus-3B:Q4_K_M | fccf0d339fc7 | 2.0 GB | 8 days ago | McCoy finance |
| qwen3.5:9b | 6488c96fa5fa | 6.6 GB | 8 days ago | Dax/Data/Neo/Ollie/Chekov |
| llama3.1:latest | 46e0c10c039e | 4.9 GB | 10 days ago | Uhura earnings |
| 0xroyce/plutus:latest | 83f2e56702ad | 5.7 GB | 2 weeks ago | McCoy (full) |
| qwen2.5-coder:7b | dae161e27b0e | 4.7 GB | 2 weeks ago | Data coder |
| kimi-k2.5:cloud | 6d1c3246c608 | — | 4 weeks ago | Cloud only |
| deepseek-v3.1:671b-cloud | d3749919e45f | — | 6 weeks ago | Cloud only |
| gemma3:4b | a2af6cc3eb7f | 3.3 GB | 7 weeks ago | Lightweight |

**G1 Action:** Install Ollama on G1, then `ollama pull` each local model. Cloud models (kimi-k2.5, deepseek-v3.1) need no transfer. Verify G1 has sufficient VRAM/RAM for 14B models.

---

## 4. launchd Plists → systemd Units

20 plists in `~/Library/LaunchAgents/`. All need conversion to systemd `.service` + `.timer` units.

### 4.1 com.trademinds.* (Core Services)

| Plist | Script | Trigger | Notes |
|-------|--------|---------|-------|
| com.trademinds.trader | main.py | Continuous (KeepAlive) | **MAIN SERVICE** — highest priority |
| com.trademinds.watchdog | watchdog.py | Continuous | Monitors main service |
| com.trademinds.scanner | engine.fast_scanner --daemon | Continuous | Market scanner daemon |
| com.trademinds.healthcheck | healthcheck.py | StartCalendarInterval | Health monitoring |
| com.trademinds.crew | main_crew.py | Continuous | Uses .venv-crew interpreter |
| com.trademinds.mcp | engine/mcp_server.py | Continuous | MCP server |
| com.trademinds.signal-center | signal-center/server.py | Continuous | Port 9000, bound 127.0.0.1 |
| com.trademinds.tunnel | cloudflared tunnel | Continuous | Cloudflare tunnel to :8080 |
| com.trademinds.premarket | premarket-scan.sh | StartCalendarInterval | bash script |
| com.trademinds.caffeinate | /usr/bin/caffeinate -s -t 39600 | StartCalendarInterval | macOS ONLY — no G1 equivalent needed |

### 4.2 com.ollietrades.* (Agent Schedules)

| Plist | Script | Trigger | Notes |
|-------|--------|---------|-------|
| com.ollietrades.archer-briefing | engine/archer_morning_synthesis.py | StartCalendarInterval | Uses venv |
| com.ollietrades.danelfin-update | engine/danelfin_weekly_cron.py | StartCalendarInterval (weekly) | Uses venv |
| com.ollietrades.etfregime | /Users/bigmac/ollietrades/etf_regime_trader.py | StartCalendarInterval | **OLD PATH** — not in autonomous-trader |
| com.ollietrades.ghost-trader | engine/ghost_trader.py daemon | Continuous | Uses venv |
| com.ollietrades.morningbriefing | /Users/bigmac/ollietrades/morning_briefing.py | StartCalendarInterval | **OLD PATH** + **⚠️ HARDCODED ALPACA KEYS IN PLIST XML** |
| com.ollietrades.nightly-backtest | engine/nightly_backtest.py | StartCalendarInterval | Uses venv |
| com.ollietrades.optionsflow | /Users/bigmac/ollietrades/options_flow_scanner.py | StartCalendarInterval | **OLD PATH** |
| com.ollietrades.riker-synthesis | engine/riker_synthesis.py | StartInterval (interval) | Uses venv |
| com.ollietrades.uhura | agents/uhura_agent.py | StartCalendarInterval | Uses venv |

### 4.3 com.papertrader.* (Legacy)

| Plist | Script | Port | Notes |
|-------|--------|------|-------|
| com.papertrader.server | uvicorn main:app | 8000 | ~/paper-trader dir, uses system Python 3.9 — assess if needed on G1 |

**G1 Action:**
- Convert StartCalendarInterval → `systemd .timer` OnCalendar
- Convert StartInterval → `systemd .timer` OnUnitActiveSec
- KeepAlive=true → `Restart=on-failure` in `.service`
- Skip `caffeinate` (macOS power management — not needed on server)
- Fix 3 plists pointing to OLD PATH (`/Users/bigmac/ollietrades/`) — verify if these scripts are still used
- **CRITICAL:** Rotate Alpaca keys exposed in `com.ollietrades.morningbriefing.plist` before migration

---

## 5. Cron Jobs

```
0 6 * * *  /Users/bigmac/autonomous-trader/dr_crusher.sh   # Daily 6 AM
0 * * * *  /Users/bigmac/autonomous-trader/dr_crusher.sh   # Hourly
```

**Script:** `dr_crusher.sh` — memory/resource cleanup (name suggests MemGuard cleanup role)

**G1 Action:** Convert to systemd timer or keep as cron (both work on Ubuntu).

---

## 6. Environment Variables / Secrets

**43 variables in `/Users/bigmac/autonomous-trader/.env`** (keys only — values redacted):

```
ALPACA_API_KEY
ALPACA_SECRET_KEY
ALPHA_VANTAGE_KEY
ANTHROPIC_API_KEY
APCA_API_KEY_ID
APCA_API_SECRET_KEY
CAPTAIN_PIN
CREWAI_CODE_MODEL
CREWAI_MODEL
DASHBOARD_PASS
DASHBOARD_ROLE
DASHBOARD_USER
DASHBOARD_USERS
FINNHUB_API_KEY
FINVIZ_EMAIL
FINVIZ_PASSWORD
FRED_API_KEY
GEMINI_API_KEY
GROK_MODEL
GROQ_API_KEY
KIRK_PORTFOLIO_CASH
KIRK_PORTFOLIO_VALUE
NTFY_ADMIN_TOPIC
NTFY_CREW_TOPIC
OLLAMA_BASE_URL
OPENAI_API_KEY
OPENAI_CODEX_MINI_MODEL
OPENAI_CODEX_MODEL
PASSWORD
POLYGON_API_KEY
SHIP_COMPUTER_USE_CLAUDE_API
TELEGRAM_BOT_TOKEN
TELEGRAM_CHAT_ID
TOTP_SECRET
TRADEMINDS_API_KEY
TRADEMINDS_DB
TRADEMINDS_MCP_KEY
TRADEMINDS_SECRET
TRADING_MODE
USERNAME
WEBULL_ACCOUNT_ID
WEBULL_APP_KEY
WEBULL_APP_SECRET
XAI_API_KEY
```

**⚠️ SECURITY NOTE:** `com.ollietrades.morningbriefing.plist` contains raw Alpaca API key and secret in its XML (visible in plist ProgramArguments). These should be rotated before the G1 migration and removed from the plist — use `.env` instead.

**G1 Action:** Transfer `.env` file securely (scp, not git). Do NOT commit secrets to repo.

---

## 7. Sacred Databases

| Database | Path | Size | Status | Notes |
|----------|------|------|--------|-------|
| trader.db | data/trader.db | 137 MB | SACRED — NEVER delete | Primary production DB, WAL mode |
| arena.db | data/arena.db | 0 B | SACRED — NEVER delete | Empty but reserved |
| signals.db | signal-center/signals.db | unknown | Active | Signal Center DB |
| autonomous_trader.db | (project root) | unknown | Check if active | Possibly legacy |
| scanner.db | (project root) | unknown | Check if active | Scanner DB |
| trading.db | (project root) | unknown | Check if active | Possibly legacy |
| backtest_results.db | data/backtest_results.db | unknown | Historical | Backtest storage |

**Key trader.db stats (as of 2026-04-16):**

| Table | Rows |
|-------|------|
| trades | 1,378 |
| signals | 51,963 |
| institutional_holdings | 18,199 |
| bridge_votes | 88 |
| volume_alerts | 278,784 |
| breadth_snapshots | 5,159 |
| backtest_history | 121 |
| trade_block_log | 1 |

**G1 Action:** `rsync` or `scp` all databases. Preserve WAL files (`.db-wal`, `.db-shm`) if present. Never `rm` or truncate. Test with read-only query after transfer.

---

## 8. Cache Directories

| Cache | Path | Size | Notes |
|-------|------|------|-------|
| Ollama models | ~/.ollama/models | 57 GB | Transfer via `ollama pull` not file copy |
| EDGAR cache | ~/.edgar/_cache | 3.3 MB | Regenerates automatically |
| Backtest cache | data/backtest_cache | 196 KB | Per-(agent, sym, date) Ollama decision cache — transfer to preserve cache hits |
| yfinance cache | ~/.cache/yfinance | Not found | Not present on this machine |

**G1 Action:** Transfer `data/backtest_cache` to preserve Ollama decision cache. Let EDGAR cache regenerate.

---

## 9. Tunnel / External Services

### Cloudflare Tunnel

```yaml
# ~/.cloudflared/config.yml
tunnel: dee0002c-c451-4919-8b16-d649ad19d029
credentials-file: /Users/bigmac/.cloudflared/dee0002c-c451-4919-8b16-d649ad19d029.json

ingress:
  - hostname: bridge.ollietrades.com
    service: http://localhost:8080
    originRequest:
      noTLSVerify: true
  - hostname: bridge.accessapple.com
    service: http://localhost:8080
  - service: http_status:404
```

**G1 Action:**
1. Install cloudflared on G1
2. Copy credentials JSON to G1
3. Copy config.yml (update paths from /Users/bigmac → /home/user or equivalent)
4. Create systemd service for cloudflared (replaces com.trademinds.tunnel.plist)
5. DNS routes both domains to tunnel — no DNS changes needed

### Signal Center

- Runs on `localhost:9000` (bound to 127.0.0.1 only)
- Internal API at `/api/signal`
- Plist: `com.trademinds.signal-center`

---

## 10. Git State

| Item | Value |
|------|-------|
| Branch | main |
| Latest commit | `242d4abd` — fix: move Captain's Portfolio to sidebar position 4 (S6.3) |
| Remote | git@github.com:accessapple2/BigMac.git |
| Auth | SSH key (`git@github.com`) |

**G1 Action:**
- Install Git on G1
- Add G1 SSH public key to GitHub (accessapple2 account)
- `git clone git@github.com:accessapple2/BigMac.git autonomous-trader`

---

## 11. Third-Party Dependencies Not via pip

### Homebrew Formulas (macOS-specific — assess each for G1)

| Formula | G1 Equivalent | Notes |
|---------|---------------|-------|
| cloudflared | `apt install cloudflared` or download binary | Tunnel service |
| python@3.12 | `apt install python3.12` | For .venv-crew |
| python@3.14 | Build from source or deadsnakes PPA | For .venv, scrapling-venv |
| openssl@3 | `apt install libssl-dev` | Usually bundled |
| sqlite | `apt install sqlite3 libsqlite3-dev` | Required |
| ffmpeg | `apt install ffmpeg` | Media processing |
| nmap | `apt install nmap` | Network scanning |
| deno | `apt install deno` or download binary | JavaScript runtime |
| uv | `pip install uv` or binary download | Python package manager |
| rtk | Build from source / binary | Token killer CLI |
| pipx | `apt install pipx` | Python app manager |
| yt-dlp | `pip install yt-dlp` or `apt install yt-dlp` | Video downloader |

**macOS-only / skip on G1:**
- pyobjc-core, pyobjc-framework-Cocoa (remove from pip requirements)
- caffeinate (macOS power management — not needed on server)
- little-cms2, dav1d, svt-av1, libvpx, opus, lame, x264, x265 (multimedia codecs — only if ffmpeg needs them)

### System-level

- **Ollama** — install via `curl https://ollama.ai/install.sh | sh`
- **Playwright browsers** — `playwright install chromium` (or all browsers) after pip install

---

## 12. Known Issues

### Active Bugs (document before migration — do not carry to G1 silently)

| Issue | Severity | Location | Description |
|-------|----------|----------|-------------|
| MSFT double-write | MEDIUM | ollie-auto execution path | MSFT sell trades appear as duplicate rows in DB (same qty/price/time). Double-write bug in trade execution. |
| Kirk NoneType | LOW | ollie_commander.py | `Kirk advisory error: '<' not supported between instances of 'NoneType' and 'int'` — None value reaching comparison operator. |
| MemGuard RAM | HIGH | MemGuard / bigmac | Only 439–474 MB free vs 2048 MB needed. Repeatedly force-killing Ollama models. G1 should have more RAM headroom — verify before migrating 14B models. |
| morningbriefing plist keys | CRITICAL | com.ollietrades.morningbriefing.plist | Raw Alpaca API key and secret embedded in plist XML ProgramArguments. Rotate before migration. |
| 3 plists point to old path | MEDIUM | etfregime, morningbriefing, optionsflow | These plists reference `/Users/bigmac/ollietrades/` not `/Users/bigmac/autonomous-trader/`. Scripts may not exist or may be outdated. |

---

## 13. Dashboard / Service URLs

| Service | URL | Port | Auth |
|---------|-----|------|------|
| Dashboard (external) | https://bridge.ollietrades.com | 443 (via tunnel) | Login: Sniff (case-sensitive) |
| Dashboard (external alt) | https://bridge.accessapple.com | 443 (via tunnel) | Same credentials |
| Dashboard (local) | http://localhost:8080 | 8080 | — |
| Signal Center | http://localhost:9000 | 9000 | Bound to 127.0.0.1 |
| Paper Trader (legacy) | http://127.0.0.1:8000 | 8000 | ~/paper-trader |
| Ollama API | http://localhost:11434 | 11434 | OLLAMA_BASE_URL in .env |

---

## Migration Priority

### Priority 1 — BEFORE migration (do on BigMac first)

1. **Rotate Alpaca keys** — keys exposed in `com.ollietrades.morningbriefing.plist` XML. Rotate via Alpaca dashboard, update `.env`, fix plist to use env vars.
2. **Final DB backup** — `rsync -av data/trader.db data/arena.db ~/G1_BACKUP/`
3. **Full pip freeze** — run `venv/bin/pip freeze > docs/requirements_venv_frozen.txt` and same for other 3 venvs.
4. **Verify old-path plists** — check if etfregime, morningbriefing, optionsflow scripts in `~/ollietrades/` are still active or dead code.

### Priority 2 — G1 Setup (infrastructure first)

1. Install Ubuntu, configure SSH access, set hostname
2. Install Git + clone repo via SSH
3. Install Ollama + pull all 13 local models (~57GB transfer time)
4. Install Python 3.9.6 (or compatible), 3.12.13, 3.14.3 — rebuild all 4 venvs
5. Install Homebrew equivalents (cloudflared, sqlite, ffmpeg, etc.)
6. Transfer `.env` securely
7. Transfer databases (`rsync trader.db arena.db` + other DBs)
8. Transfer `data/backtest_cache/`
9. Install + configure cloudflared tunnel

### Priority 3 — Service Migration (ordered)

1. Start `main.py` (com.trademinds.trader) — core dashboard
2. Start Signal Center (port 9000)
3. Start cloudflared tunnel — verify bridge.ollietrades.com loads
4. Start scanner daemon, watchdog, MCP server
5. Start scheduled agents (premarket, healthcheck, etc.)
6. Start crew service (main_crew.py / .venv-crew)
7. Migrate scheduled plists → systemd timers (archer-briefing, danelfin, nightly-backtest, uhura, riker)
8. Test full trade cycle (paper trading only)

### Priority 4 — Deprecate / Skip on G1

- `caffeinate` plist — macOS power management, not needed on server
- `com.papertrader.server` — assess if ~/paper-trader is still in use
- `pyobjc-*` packages — remove from G1 venv requirements
- `com.ollietrades.etfregime` / `morningbriefing` / `optionsflow` — verify old scripts are still needed before migrating

---

*Inventory generated 2026-04-16 by Claude Code (claude-sonnet-4-6).*
*Source: read-only audit of /Users/bigmac/autonomous-trader and system state.*
