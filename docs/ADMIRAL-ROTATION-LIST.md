# ADMIRAL ROTATION LIST — secrets exposed in quarantined `.env.bak` (2026-06-10)

**Context (HM-HARDEN A4).** `.env.bak` (snapshot 2026-05-25) was quarantined out
of the repo to `~/secure_archive/env.bak.2026-06-10` (chmod 600, **never
deleted**). While it lived in the working tree it carried live secret VALUES.
**Rotation of the LIVE credentials below is an Admiral action** — Claude does not
rotate live secrets autonomously. **Names only here; no values are recorded.**

**"24 differing keys" reconciliation:** the HM-LEDGER "24" was imprecise. Exact
diff vs current `.env`: **6 keys hold a *different* (stale) value** in `.env.bak`
(`ADVISORY_OLLAMA_URL`, `OLLAMA_BASE_URL`, `OLLAMA_URL`, `GROK_MODEL`,
`CONVICTION_SCALED_STOPS_ENABLED`, `CONVICTION_SCALED_TRAIL_ENABLED` — all
config, **not secrets**); the other 18 are newer keys absent from the `.bak`.
So the **value-drift is in non-secret config**, but the `.bak` still contained
the *current* values of the real credentials below — those are the exposure that
matters, regardless of drift.

## P1 — rotate first (broker / auth / 2FA; blast radius = money or login)
| Key (name only) | Live/Dead | Note |
|---|---|---|
| `APCA_API_KEY_ID` | LIVE | Alpaca paper broker — the only account the fleet trades. |
| `APCA_API_SECRET_KEY` | LIVE | Alpaca paper secret. |
| `TOTP_SECRET` | LIVE | Dashboard 2FA seed — compromise defeats 2FA. |
| `CAPTAIN_PIN` | LIVE | Captain trader-login PIN **and** the signal-center→trader handshake (now mirrored by `SIGNAL_CENTER_PIN`). Rotate BOTH together; you must know the new value before signal-center restarts. (HM-HARDEN A3 removed the hard-coded `"2026"` literal from `signal-center/server.py`; value still env-driven.) |
| `SIGNAL_CENTER_PIN` | LIVE (new) | Must equal `CAPTAIN_PIN`. Added 2026-06-10. |
| `TRADEMINDS_SECRET` | LIVE | Flask session signing key — rotation invalidates sessions. |
| `DASHBOARD_PASS` | LIVE | Dashboard password. |
| `PASSWORD` | LIVE | Generic auth password (confirm consumer). |

## P2 — rotate next (paid API keys; blast radius = spend / data access)
| Key (name only) | Live/Dead | Note |
|---|---|---|
| `ANTHROPIC_API_KEY` | LIVE | Claude API (CIC / ship-computer). |
| `OPENAI_API_KEY` | LIVE | OpenAI (mostly local-redirected, but key is real). |
| `XAI_API_KEY` | LIVE | Grok advisor + Kirk-briefing synthesis. |
| `POLYGON_API_KEY` | LIVE | Polygon Stocks+Options Starter (paid). |
| `GEMINI_API_KEY` | LIVE | Gemini. |
| `GROQ_API_KEY` | LIVE | Groq. |
| `FINNHUB_API_KEY` | LIVE | Finnhub (fallback chain). |
| `ALPHA_VANTAGE_KEY` | LIVE | Alpha Vantage (fallback chain). |
| `FRED_API_KEY` | LIVE | FRED (optional; keyless CSV fallback exists). |
| `TRADEMINDS_API_KEY` | LIVE | Internal API key. |
| `TRADEMINDS_MCP_KEY` | LIVE | MCP server key. |
| `AI4TRADE_TOKEN` | LIVE? | Confirm consumer before rotating. |

## P3 — rotate when convenient (integrations / lower blast radius)
| Key (name only) | Live/Dead | Note |
|---|---|---|
| `TELEGRAM_BOT_TOKEN` | LIVE? | Telegram bot (confirm still used). |
| `TI_GMAIL_APP_PASSWORD` | LIVE | TI email ingest (Gmail app password). |
| `FINVIZ_PASSWORD` | LIVE? | Finviz Elite (confirm). |

## DEAD — do NOT rotate (account wound down; remove from `.env` at leisure)
| Key (name only) | Status |
|---|---|
| `WEBULL_APP_KEY` | DEAD — Webull liquidated 2026-04-17. |
| `WEBULL_APP_SECRET` | DEAD — Webull liquidated. |
| `WEBULL_ACCOUNT_ID` | DEAD — Webull liquidated. |

## NOT credentials (no rotation — listed so they're not mistaken for secrets)
Config/flags present in `.env.bak`: `OLLAMA_URL`, `OLLAMA_BASE_URL`,
`ADVISORY_OLLAMA_URL`, `GROK_MODEL`, `CREWAI_MODEL`, `CREWAI_CODE_MODEL`,
`TROI_MODEL`, `OPENAI_CODEX*_MODEL`, `KIRK_*`, `MINERVINI_FILTER_ENABLED`,
`RS_RANK_ENABLED`, `*_SQUEEZE_WATCHER_ENABLED`, `CONVICTION_SCALED_*`,
`TRADING_MODE`, `DASHBOARD_USER`/`USERNAME`/`DASHBOARD_ROLE`/`DASHBOARD_USERS`,
`NTFY_*_TOPIC`, `TI_IMAP_HOST`/`TI_IMAP_PORT`, `TI_GMAIL_USER`/`FINVIZ_EMAIL`
(identifiers), `TRADEMINDS_DB`, `SHIP_COMPUTER_USE_CLAUDE_API`.

— HM-HARDEN A4, 2026-06-10. Quarantined file: `~/secure_archive/env.bak.2026-06-10` (600).
