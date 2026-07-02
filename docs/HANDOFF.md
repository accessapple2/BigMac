# OLLIETRADES HANDOFF v2

Tier 1 = repo-verified. Tier 2 = carried from prior context, confirm before acting.

---

## TIER 1 — VERIFIED THIS SESSION (confirmed against repo/live origin)

### Repo root
`/Users/bigmac/autonomous-trader` — confirmed via `pwd`, twice, in this session. (Not `~/ollietrades` or `~/OllieTrades` — those are legacy pre-rename paths still present on disk but not the live repo.)

### Cloudflare tunnel — ingress → service map
Tunnel `dee0002c-c451-4919-8b16-d649ad19d029`, config at `~/.cloudflared/config.yml`:

| Hostname | Service |
|---|---|
| bridge.ollietrades.com | http://localhost:8080 |
| signal.ollietrades.com | http://localhost:9000 |
| swingdesk.ollietrades.com | http://localhost:8889 |
| tour.ollietrades.com | http://localhost:8088 |
| (catch-all) | http_status:404 |

**Apex decision:** `ollietrades.com`/`www` are NOT in this ingress map — confirmed by direct read of the config. The apex-DNS fix was applied as a Cloudflare edge Redirect Rule (2 proxied DNS records + 1 rule), not a tunnel-ingress change.

**Note on local vs. remote tunnel config:** the Admiral reports (from the live dashboard, not independently checked by me) that the tunnel has **5** hostnames configured remotely, including `otasty.ollietrades.com` which is NOT in the local `~/.cloudflared/config.yml` above. The local file is evidently not authoritative — CLAUDE.md separately describes the tunnel as "Remote config v11," consistent with this. `otasty` was an orphaned record (proxied DNS, no real ingress target) and was removed — confirmed independently: `dig otasty.ollietrades.com` now returns NXDOMAIN.

**Full remote hostname list — NOT YET PULLED, blocked.** `arena.ollietrades.com` reportedly resolves and serves the Signal Center live, but appears nowhere in the local config.yml (which only has 4 entries) — a real discrepancy, not yet reconciled. Attempted to pull the authoritative remote ingress list this session via:
- `cloudflared tunnel list` / `cloudflared tunnel info <id>` — both work (confirms the tunnel and active connectors) but neither exposes the actual ingress/hostname rules for a remotely-managed (dashboard v11) tunnel; no "dump remote config" subcommand exists in this cloudflared version (2026.6.0)
- Cloudflare dashboard via browser automation — blocked twice more this session: the previously-working tab closed, and a fresh tab hit "Permission denied for this action on this domain" on `one.dash.cloudflare.com` (a different subdomain than the one previously granted extension access; needs a fresh per-site grant I can't self-authorize)
- No Cloudflare API token access (reading `.env` is blocked by the HM-SHIELDS paper-only-invariant hook, by design)

**Still needed:** paste of the Tunnel's public-hostname configuration screen (same approach that worked for the DNS records list), so the full remote hostname → service map (all 5+, including `arena` and `otasty`'s former entry) can be reconciled against the local file and documented here properly.

### DNS apex + redirect — APPLIED, independently verified live
Applied directly in the Cloudflare dashboard (browser automation on that page was non-functional all session — every attempt hung on a stuck "page never idle" state). I verified the result myself from this shell, not just from the report:

```
$ dig +short ollietrades.com A          → 172.67.208.56, 104.21.45.31 (Cloudflare proxy)
$ dig +short www.ollietrades.com        → 104.21.45.31, 172.67.208.56 (Cloudflare proxy)
$ curl -I http://ollietrades.com/       → 301, location: https://bridge.ollietrades.com/
$ curl -I https://www.ollietrades.com/  → 301, location: https://bridge.ollietrades.com/
$ curl -I ".../foo?bar=1&baz=2"         → 301, location: https://bridge.ollietrades.com/foo
```

Apex and www both resolve and both 301 to bridge with real `cf-ray` headers (not cached/local) — genuinely live.

**Gap found, not in the original report:** path is preserved but **query string is dropped** — `?bar=1&baz=2` did not survive the redirect, despite "preserve query string ON" being part of the original spec. Worth a follow-up if any inbound links to the apex ever carry query params (e.g. UTM tags, referral codes).

### Origin services — live, confirmed via `lsof`
- `:8080` — PID 72842, `main.py` (trader/dashboard, `.venv` Python 3.14)
- `:9000` — PID 60260, `signal-center/server.py` (separate `venv` Python 3.9)
- `:8889` — PID 89032, SwingDesk (`swingdesk/backend.py`) — restarted 2026-07-02, see incident below
- `:8088` — PID 417, `tour_api.py` — healthy, unaffected by the SwingDesk incident

### INCIDENT — SwingDesk 502, 2026-07-02 (RESOLVED)
`swingdesk.ollietrades.com` was returning 502 Bad Gateway. Diagnosed and fixed:

**Symptom:** the old process (PID 287, uptime 1d+) was still alive and still `LISTEN`ing on `:8889` per `lsof` — not crashed. But a direct `curl -v http://127.0.0.1:8889/` showed the TCP handshake completing, the HTTP request being sent, then `Recv failure: Connection reset by peer` — the OS-level socket accepted the connection but the application couldn't complete it.

**Root cause:** `logs/otasty_error.log` showed repeated `OSError: [Errno 24] Too many open files` (22 occurrences) from `asyncio`'s `socket.accept()`. `lsof -p 287` showed 303 open file descriptors against a 256 soft `maxfiles` limit. `logs/otasty.log` showed the actual leak source: **689** repeated occurrences of `[otasty-shadow-scheduler] OperationalError: table swingdesk_shadow_trades has no column named broker_order_id` — a background scheduler job hitting a DB schema mismatch on every cycle. Each failed cycle appears to leak a file descriptor (likely an unclosed DB connection/cursor in the exception path, not confirmed via source read this pass); over ~1 day of uptime this exhausted the process's FD limit, after which the asyncio event loop could no longer `accept()` new connections — matching "tunnel up, origin not responding" exactly, and explaining the connection-reset behavior rather than a clean crash.

**Fix applied:** `bash scripts/swingdesk_restart.sh` — kills the old process, relaunches `swingdesk/backend.py` fresh (new PID 89032/89061). Verified both locally (`curl localhost:8889/api/health` → 200) and live through the tunnel (`curl -I https://swingdesk.ollietrades.com/` → 302, same healthy CF-Access-redirect pattern as before the incident, not a 502).

**Not fixed — will recur:** the underlying `swingdesk_shadow_trades` table is missing a `broker_order_id` column that the shadow-scheduler code expects. This will keep failing on every scheduler cycle and will very likely leak file descriptors again over the next ~1 day of uptime, causing the same 502 to recur unless the schema mismatch itself is fixed (a migration adding the missing column) or the scheduler's exception handling is fixed to not leak on failure. Not touched this pass — wasn't asked for, and a DB schema change to a live table warrants its own explicit go-ahead.

**`tour` (:8088) does not share this fate** — checked directly: only 51 open FDs (vs SwingDesk's 303) despite similar ~1-day uptime, `logs/tour_api.log` has zero occurrences of either error pattern, and it responds locally in ~1ms. Different codebase (`tour_api.py`, standalone), no shared scheduler.

### DB inventory (repo-scoped `find`)
- **Canonical / sacred (never-delete per CLAUDE.md):** `data/trader.db`, `data/arena.db`, `signal-center/signals.db`
- **Secondary feature DBs:** `data/flow_gex.db`, `data/deep_scan.db`, `data/ghost_trades.db`, `data/backtest_results.db`, `data/backtest.db`, `data/alpha_signals.db`, `data/proving_ground.db`, `data/uhura_research.db`, `signal-center/signals_archive.db`
- **Stale/archival (NOT live):** root-level `trader.db`, `autonomous_trader.db`, `swingdesk.db`, everything under `backups/`

### Shipped fixes this session — commit hashes (branch `exec-pipeline`)
1. `75a3684` — canonical fleet P&L source of truth across bridge v1/v2/signal-center
2. `5cfdc97` — Fear & Greed null-guard (Ship's Log rendering unavailable as 0)
3. `2cf39da` — Battle Tab Trade Desk avg-cost field mapping
4. `a3df4e6` — leaderboard equity anchor hardcoded to season=5
5. `c23312b` — Riker confidence-scale bug + empty SCREENER intelligence rows
6. `8c85b78` — stop persisting risk_radar cold-start placeholder as a signal
7. `532573f` — predictions_snapshot skips incomplete rows instead of persisting them
8. `9fda946` — removed leftover LiveChart MutationObserver debug trap
9. `a6bfec6` — surface failed admin actions (Matrix tab: ntfy + red styling + honest status)
10. `511246a` — removed duplicate fetch calls on bridge-v2 initial load
11. `5d67c14` — GEX endpoints hang on cold cache — bound + serve-stale-while-revalidate
12. `13f0f17` — Chart.js 3.9.1 → 4.4.1
13. `ac92b38` — Fleet Report Card collapses dormant agents, sorts by P&L
14. `e23b3e0` — empty-state polish for Phaser-Lock (skeleton loader)
15. `4c19eb9` — v1/v2 P&L divergence fix + gex_all() cache-miss fix + StructureFix noise silenced
16. `0e1d485` — bridge-v2 is now the root view, v1 demoted to /classic
17. `3a6f102` — relocated 6 misplaced sections into `.main`, removed StructureFix shim entirely

Plus (not a git commit — crontab, verified live): Riker synthesis re-homed to `*/10 * * * *` cron (`engine/riker_synthesis.py`); manually run once, confirmed a fresh `rikers_log` row written 2026-07-02 (first update since 2026-05-23).

### Push state — corrected (this line was stale; see below)
**Pushed to `origin/exec-pipeline`.** All 18 commits above (`75a3684` through `1d2bca2`, including this handoff doc itself) are on the remote. Verified directly: `git log origin/exec-pipeline..exec-pipeline --oneline` returns empty, and `git rev-parse HEAD` == `git rev-parse origin/exec-pipeline` == `1d2bca2f8a783a20718003e75835969e1c008a4d`, confirmed again after a fresh `git fetch` (not a stale local cache).

**When/under what authorization:** the standing rule for this session was no-auto-push. The Admiral explicitly requested a one-off exception ("Push docs/HANDOFF.md to origin now — one-off confirmed") later in the same session, after this doc's original text (claiming nothing was pushed) had already been written and committed. The push itself was disclosed at the time as pushing the *entire branch* (git push moves the branch pointer, not a single file) — all 18 commits went up together as a result of that one authorized push, not 18 separate authorizations.

### Open INFRA sign-offs (Admiral gate)
1. ~~DNS apex record + redirect~~ — **DONE**, verified live. Query-string-preservation fix **attempted this session, blocked** — same Cloudflare-dashboard access problem as the tunnel hostname list above (browser automation non-functional). The fix itself is well-understood (enable "preserve query string" on the existing Redirect Rule, or switch to a dynamic expression that appends `http.request.uri.query` if that flag alone isn't taking effect) — just need working dashboard access to apply and re-verify with `curl -I "http://ollietrades.com/foo?bar=1"` → location should end `?bar=1`.
2. **Origin stability** — queued, not started. Per repo doctrine (`LaunchAgent Reboot Lifecycle` in CLAUDE.md), the correct mechanism here is **cron** (`@reboot` + interval entries), not launchd/systemd — launchd/LaunchDaemon bootstrap is documented as broken on this box over SSH-only sessions. Naming corrected per Admiral direction 2026-07-02; do not reintroduce systemd/launchd framing for this item.
3. **manifest.json CF Access bypass** — queued, not started
4. **status.ollietrades.com** — queued, not started

### O-Tasty — verified against repo source
The Admiral identified `otasty` as an internal codename for an in-house tastytrade-style options engine inside SwingDesk. Confirmed directly in the repo (not just from the report):
- `swingdesk/options_engine.py`, `swingdesk/shadow_autopilot.py`, `swingdesk/scanner.py`, `swingdesk/backend.py` all reference it; also `tests/test_otasty_shadow_invariants.py`, `tests/test_otasty_invariant_tamper.py`
- `swingdesk/shadow_autopilot.py:237` — `LIVE_EXECUTION_ENABLED = False   # PA3YVDTUH5CB paper only; never fleet account; never real money` — literal source, confirms both the isolated Alpaca paper account ID and the execution-disabled default
- Reached via SwingDesk's Test Kitchen tab, no dedicated subdomain needed — consistent with `otasty.ollietrades.com` being an orphaned/unused DNS record rather than a real routing target

### Cloudflare account details (Admiral-reported from the dashboard, not independently checkable by me)
- Zone `ollietrades.com`, Zone ID `0efaf9b4eba9ba61e16c4eec9e96740f`, Account ID `4925885a5424f7948d21d5e6ffaad234` ("Bonnie's Account"), Free plan, registrar Cloudflare, expires 2027-04-08
- **"Agent Lee" / "Enable Agent Lee access"** — flagged by me as suspicious when it first appeared (I hadn't seen it anywhere in this project). The Admiral identified it as Cloudflare's own first-party in-dashboard "Ask AI" feature, which auto-generates a read-only token if enabled, and left it **disabled**. I have no way to independently confirm this from outside the dashboard — noting it here as the Admiral's direct account-level observation, not something I verified myself. If anyone revisits this, worth a second look before trusting it's benign purely on the strength of this note.

### Ticket codes confirmed against CLAUDE.md
- `HM-ORPHAN-SEATS` — CLAUDE.md:312 ("11 `ai_players` seats referencing Ollama models absent from olliemax")
- `HM-SHELLY-WATCHDOG` — CLAUDE.md:225-226, 253 (Shelly plug watchdog scope, sketched not built, post-trip)

---

## TIER 2 — CARRIED OVER, NOT VERIFIED THIS SESSION (from prior/XO standing context; next session must confirm)

- **Ollie Max hardware:** MSI Aegis ZS2 / Ryzen 9 9900X / RTX 5080 16GB / Ubuntu 26.04 — not checked against any live system this session (only its Ollama endpoint, `192.168.1.168:11434`, is referenced in repo config/CLAUDE.md)
- **Machine roster:** Ollie Box (.166, RETIRED), laptop (.177), Bonnie's Mini = Minisforum — none of these appear anywhere in this session's tool output or in CLAUDE.md
- **bigmac IP** — provided as 192.168.1.248; CLAUDE.md's Shelly plug table instead lists bigmac at **192.168.1.245**. Discrepancy, not reconciled — confirm actual IP before relying on either.
- **Ticket backlog not found in repo:** `HM-DAX`, `HM-VOTE-AUDIT`, `HM-PY-CONSOLIDATE`, `HM-LESSON-SHADOW`, `HM-SCORECAP-REVISIT`, `HM-WAL-ROOTCAUSE` — zero grep hits in CLAUDE.md or docs/ this session
- **cloudflared launch mechanism** — carried-over text described it as "a system LaunchDaemon"; CLAUDE.md instead documents it as an `@reboot` cron entry (`scripts/cloudflared_reboot_start.sh`), specifically because LaunchAgent/LaunchDaemon bootstrap fails on this box over SSH-only sessions. Treat the cron-based description as the confirmed one; the LaunchDaemon framing is superseded/incorrect per repo doctrine.
- **Broker posture / naming conventions:** RULE #1 (Schwab read-only, no order path) and Alpaca-paper-only ARE backed by CLAUDE.md and should be treated as Tier 1 in spirit — restated here only because the rest of that section's phrasing ("Naming: Star Trek — Admiral/XO/Scotty") wasn't independently verified; this session's own directive used Admiral + Scotty (Chief Engineer), no "XO" role appeared.
