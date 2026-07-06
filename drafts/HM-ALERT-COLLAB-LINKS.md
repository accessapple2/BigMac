# HM-ALERT-COLLAB-LINKS — Shareable One-Paste Alert Links

**Status:** DRAFT (plan only — no code, no schema, no .db writes in this pass)
**Date:** 2026-07-06
**Prior art:** Trade Ideas Pro "Collaborate" + Cloud share features
**Doctrine touchpoints:** TWO-TIER BRIDGE, CARRIER DOCTRINE Rung 1 (FIND), RULE #1 (untouched)

---

## 1. Objective

Let a user (or agent report) share an alert *configuration* as a single
copy-pasteable string/URL. Recipient pastes it once and the alert config is
recreated on their side — no manual field-by-field re-entry. Modeled on Trade
Ideas Pro's Collaborate feature.

## 2. Prior art — how Trade Ideas does it

From the TI user guide (ch. 9.14 Collaborate) and help center ("Using the
Cloud Features"):

- **Copy & Paste:** right-click an Alert / Top List / Multi-Strategy window →
  Collaborate → **Copy All** puts a *collaboration link* on the clipboard.
  The link fully encodes the scan/alert settings.
- **Paste-to-load:** right-click an existing Alert window → Collaborate →
  delete the URL in the top field, paste the received link → OK. The window's
  old settings are erased and replaced by the shared strategy.
- **Cloud share:** File → Save or Share to Cloud stores a named layout
  server-side; the Cloud window's **Share** button yields a URL; recipient
  loads via File → Load from Cloud → paste URL → Load.
- **QR codes:** the same collaboration link rendered as a QR code — scanning
  opens a web view of the scan settings (and live output when logged in).
- Example TI cloud-alert URL shape: `trade-ideas.com/Cloud.html?code=<32-hex>`
  — i.e. server-side storage keyed by an opaque code, *not* a self-contained
  payload. The Collaborate copy-paste link, by contrast, encodes settings in
  the link itself.

Two distinct patterns, both worth supporting eventually:
**(A) self-contained encoded payload** (works offline, no storage) and
**(B) short server-stored code** (short links, revocable, requires storage).
This plan ships (A) first; (B) is a follow-on.

## 3. Current state of alert architecture (verified 2026-07-06)

- **Delivery layer exists and is good:** `engine/alert_channels.py`
  ("Phase 3.7 Unified Alert Channels") — severity-routed dispatch
  (INFO→ntfy, WARNING→+browser push, RED_ALERT→+email), per-type rate limit
  (300s), standardized topics (`ollie-premarket`, `ollie-signals`,
  `ollie-critical`, admin/crew), env-driven config, CIC toggle commands.
- **Alert *events* are logged, not configured:** `engine/dynamic_alerts.py`
  monitors trendline breaks / RSI extremes / volume spikes / MACD crossovers
  with **hardcoded thresholds** (e.g. RSI<30, genuine-cross logic per the
  2026-06-01 MRVL fix). Fired events land in `trader.db::dynamic_alerts`
  (`symbol, alert_type, message, severity, price, triggered_at`) — an event
  log with 30-min in-memory cooldown. There is **no user-facing alert
  *definition* model** (no table of "alert me when X crosses Y").
- **Signal-center (Flask, :9000, `signal-center/server.py`):** already has
  row-level data export/import — `GET /api/export/<fmt>` (csv/json, line
  ~1070) and `POST /api/import` (line ~1107) — plus session auth (TOTP,
  `pyotp`) and an `exports/` dir convention. These are *signal history*
  export, not config-level share, but they establish the serialization +
  endpoint pattern to follow.
- **Dashboard (:8080, `dashboard/app.py`):** alert read endpoints exist
  (`dynamic_alerts()` ~line 9261, active-alerts pulls at ~9270/9289/17782,
  7-day triggered query ~8871). Bridge v1 (`/classic`, `index.html`) owns the
  deep alerts panel; bridge-v2 is the lean daily driver. Both are windows on
  the same `/api/*` state (TWO-TIER doctrine).
- **Public surface:** Cloudflare tunnel routes exist (`bridge→:8080`,
  `signal→:9000`) gated by CF Access `bridge-allow` — **except the known ⚠
  TODO: `signal.ollietrades.com` Access gate unverified.** Any share-link
  landing page must not assume that gate is closed.

### Gap

No alert-config data model → nothing to serialize → nothing to share. The
share-link feature therefore has a hard prerequisite: a minimal
**alert definition** layer (Phase 1) before links (Phase 2).

## 4. Proposed design

### Link format (self-contained, pattern A)

```
OLLIE-ALERT:1:<base64url( zlib( canonical-JSON payload ) )>:<hmac-sig-16>
```

- Prefix + version field → forward-compatible parsing, easy clipboard
  detection ("paste anywhere" box can auto-recognize it).
- Payload = alert definition only (see schema below). **Never** code,
  callables, SQL, file paths, or account data.
- HMAC-SHA256 (truncated) over the payload with a server-side key —
  signal-center already ships `URLSafeTimedSerializer`-style signing for
  sessions; reuse `itsdangerous` for the whole job (it does the
  serialize+sign+b64 dance natively). Unsigned/tampered links are rejected
  on paste. Note: signature proves origin, not safety — validation below
  still applies.
- Optional URL wrapper for QR/phone: `https://signal.ollietrades.com/a#<blob>`
  (fragment, not query — keeps payload out of server logs; page JS decodes
  and offers "import"). URL wrapper is Phase 3, blocked on closing the CF
  Access TODO for the `signal` route.

### Payload schema (v1)

```json
{
  "v": 1,
  "kind": "price_level | rsi | volume_spike | macd_cross | trendline",
  "symbol": "MRVL",
  "params": {"level": 93.43, "direction": "above"},
  "severity": "info | warning | red_alert",
  "channels": ["ntfy"],
  "note": "free text ≤200 chars",
  "created_by": "user-or-agent-id",
  "created_at": "2026-07-06T12:00:00Z"
}
```

Size cap ~2 KB post-encode. `kind` is an allowlist enum mapping 1:1 onto the
existing `dynamic_alerts.py` check functions — pasted configs can only turn
on checks that already exist; a link can never introduce new behavior.

### Phases

**Phase 1 — Alert definition model (prerequisite).**
New table `alert_definitions` in `trader.db` via `CREATE TABLE IF NOT EXISTS`
migration (additive only — SACRED DATA rules; no drops, no truncates):
`id, kind, symbol, params_json, severity, channels_json, enabled, created_by,
created_at, last_triggered_at`. `engine/dynamic_alerts.py` gains a small
reader that unions user definitions with the existing hardcoded checks
(hardcoded stays default-on; definitions are additive). CRUD endpoints on
dashboard `/api/alert-defs` (GET/POST/PATCH). Deep UI lands in bridge v1
alerts panel per TWO-TIER doctrine; bridge-v2 gets display-only at most.

**Phase 2 — Encode / decode (the feature).**
- `engine/alert_share.py`: `encode_alert(defn) -> str`,
  `decode_alert(blob) -> defn` (verify sig → version check → schema
  validation → allowlist check → size check).
- v1 alerts panel: per-alert "Copy share link" button (clipboard write).
- "Paste alert link" box + `POST /api/alert-defs/import` — decodes,
  validates, inserts as **disabled** (`enabled=0`); user reviews the decoded
  config and arms it explicitly. One paste = one recreated alert. Mirrors
  TI's paste-to-load, but deliberately *not* TI's "erase old settings"
  semantics — import always creates, never overwrites.
- Duplicate guard: exact (kind, symbol, params) match → offer
  skip/duplicate choice rather than silent double-insert.

**Phase 3 — QR + web landing (optional, TI parity).**
QR render of the URL wrapper (client-side JS lib, no new backend), landing
page on signal-center showing decoded settings read-only with an Import
button. **Gated on:** verifying/enforcing CF Access on
`signal.ollietrades.com` (existing ⚠ TODO in CLAUDE.md — this feature is a
forcing function to close it).

**Phase 4 (follow-on, pattern B) — short codes.** Server-stored configs with
opaque codes (TI `Cloud.html?code=` style): shorter links, revocation,
share-count. Table `shared_alerts` keyed by 16-byte random code. Not needed
for v1 value; ship only if link length proves annoying.

## 5. Security & doctrine compliance

- **RULE #1:** nothing here touches order paths, Schwab, or any execution
  surface. Alert definitions can only feed the existing notify pipeline.
  Import path must be physically unable to create trades — enforced by
  `kind` allowlist mapping only to notify-side checks.
- **No silent catch** (Error Handling Posture): decode failures return
  explicit 4xx with reason (bad sig / bad version / schema fail / too big),
  logged; never swallow-and-ignore.
- **Injection surface:** payload is data-only JSON; `note` is length-capped
  and HTML-escaped on render; params validated per-kind (numeric ranges,
  symbol regex `^[A-Z.]{1,10}$`).
- **Rate limiting:** import endpoint rides signal-center session auth (TOTP)
  + a per-session import cap; fired alerts already rate-limit via
  `alert_channels.py` (300s/type) and `dynamic_alerts.py` cooldown (1800s).
- **SACRED DATA:** additive schema only; no changes to existing tables; this
  plan itself writes nothing.
- **TWO-TIER BRIDGE:** feature UI is v1 (`/classic`) deep-panel work;
  backend state is shared; v2 never disagrees because both read
  `/api/alert-defs`.
- **CARRIER DOCTRINE:** a pasted alert is a new **sensor contact** source —
  Rung 1 (FIND) only. No sortie path from an imported alert; Rung 4 gating
  is untouched.

## 6. Testing & rollout

- Unit: encode→decode round-trip; tamper (flip one byte → reject); version
  bump rejection; oversize rejection; per-kind param validation matrix.
- **Frontend Ship Rule (HM-BJ.E4):** the copy button and paste box are
  non-trivial frontend JS — manual browser hover/click smoke test is a
  required closure phase, not optional.
- **Restart-then-verify (HM-CONSOLE-INIT):** smoke-restart trader after the
  `dynamic_alerts.py` union change; confirm log heartbeat shows the
  definition reader firing in the live loop (Daemon Lifecycle Rule — verify
  the live path, not an import-test).
- Rollout order: Phase 1 behind a `config.py` flag
  (`ALERT_DEFS_ENABLED`, default `False`) per Feature Flags convention;
  flip after smoke; Phase 2 ships only after Phase 1 stable ≥1 week.

## 7. Open questions (Admiral input wanted)

1. Who is the sharing audience? If it's only the 3 `bridge-allow` emails,
   Phase 3/4 may be over-build — Phase 2 clipboard strings may be the whole
   feature.
2. Should agent-generated alerts (e.g. Uhura hits) auto-emit share links
   into their ntfy messages? Cheap add once `encode_alert()` exists; turns
   every notification into a TI-style crumb others can adopt.
3. HMAC key management: env var alongside SMTP/ntfy config, or reuse
   signal-center's existing secret? (Recommend a dedicated
   `ALERT_SHARE_KEY` so rotating one doesn't break the other.)
4. Telegram path in `dynamic_alerts.py` (`_send_telegram`) vs ntfy-first
   doctrine in `alert_channels.py` — worth unifying while touching this
   file, or out of scope? (Recommend: out of scope, ticket separately.)

## 8. References

- Trade Ideas Collaborate: trade-ideas.com/guide/chapter/9_14/9.14Collaborate.html
- Trade Ideas Cloud share: help.trade-ideas.com/article/91-using-the-cloud-features
- `engine/alert_channels.py` (delivery layer, topics, rate limits)
- `engine/dynamic_alerts.py` (checks + `dynamic_alerts` event table, lines 23–58)
- `signal-center/server.py` — `/api/export/<fmt>` (~1070), `/api/import` (~1107)
- `dashboard/app.py` — dynamic-alerts endpoints (~8871, ~9261–9289, ~17782)
- CLAUDE.md — TWO-TIER BRIDGE, CARRIER DOCTRINE, Feature Flags, SACRED DATA,
  Frontend Ship Rule, ⚠ signal.ollietrades.com CF Access TODO
