# HM-ALERT-COLLAB-LINKS — Shareable One-Paste Alert Links

**Status:** DRAFT — reviewed 2026-07-07; Admiral decisions recorded (§7); bundle
payload + integration notes added. Still plan-only — no code, no schema, no .db writes.
**Date:** 2026-07-06 (reviewed/updated 2026-07-07)
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
- **2026-07 TI Pro promo delta:** current marketing pitches "paste one link
  and all five alerts arm themselves — then fire the moment price triggers."
  Two implications: (a) links carry *bundles* of alerts, not one; (b) TI arms
  on paste. We adopt (a) — see bundle-native payload below. We deliberately
  reject (b) — see import semantics; auto-arming pasted content is the exact
  injection pattern §5 guards against. An "arm all" button after review gets
  the one-click UX without silent arming.

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

### Integration notes from 2026-07-07 conflict check

- **HM-WAL-BUSY-TIMEOUT-HYGIENE (commits `027fb90`, `aa55f1d`, 2026-07-06):**
  hot-path sqlite connections migrated to the shared `engine/db_conn.py`
  helper (synchronous=NORMAL, busy-timeout, FD-hygiene). `dynamic_alerts.py`
  still rolls its own `sqlite3.connect("data/trader.db")` `_conn()`. **All
  Phase 1 code (new `alert_definitions` reads/writes, and any touch to
  `dynamic_alerts.py`) MUST use `engine/db_conn.py`** — do not regress the
  98-site FD-leak sweep by adding a 99th bespoke connection site.
- **CARRIER Rung 1 contact card (night-crew, `carrier_rung1_contact.html`):**
  imported/user-defined alerts that fire should ride the same
  notification-poll payload the v2 contact card consumes, and must carry
  correct Contact Classification tiering — user price/RSI/etc. alerts are
  **ACTIONABLE** (full crumb trail); anything system/measurement-ish is
  INFORMATIONAL (no carrier treatment). Wire the tier at emit time in
  Phase 1 so Rung 1 works for free.

## 4. Proposed design

### Link format (self-contained, pattern A)

```
OLLIE-ALERT:1:<base64url( zlib( canonical-JSON payload ) )>:<hmac-sig-16>
```

- Prefix + version field → forward-compatible parsing, easy clipboard
  detection ("paste anywhere" box can auto-recognize it).
- Payload = alert definition bundle only (see schema below). **Never** code,
  callables, SQL, file paths, or account data.
- HMAC-SHA256 (truncated) over the payload with a server-side key —
  signal-center already ships `URLSafeTimedSerializer`-style signing for
  sessions; reuse `itsdangerous` for the whole job (it does the
  serialize+sign+b64 dance natively). Unsigned/tampered links are rejected
  on paste. Note: signature proves origin, not safety — validation below
  still applies. Key = dedicated `ALERT_SHARE_KEY` env var (§7 decision 3).
- Optional URL wrapper for QR/phone: `https://signal.ollietrades.com/a#<blob>`
  (fragment, not query — keeps payload out of server logs; page JS decodes
  and offers "import"). URL wrapper is Phase 3, blocked on closing the CF
  Access TODO for the `signal` route — **and deprioritized per §7 decision 1
  (audience = the 3 bridge-allow emails; clipboard strings likely suffice).**

### Payload schema (v1, bundle-native)

```json
{
  "v": 1,
  "alerts": [
    {
      "kind": "price_level | rsi | volume_spike | macd_cross | trendline",
      "symbol": "MRVL",
      "params": {"level": 93.43, "direction": "above"},
      "severity": "info | warning | red_alert",
      "channels": ["ntfy"],
      "note": "free text ≤200 chars"
    }
  ],
  "created_by": "user-or-agent-id",
  "created_at": "2026-07-06T12:00:00Z"
}
```

**Bundle-native (2026-07-07):** top-level carries an *array* of 1–20
definitions — TI Pro's promoted UX is "paste one link, five alerts arm"; a
one-alert-per-link format would trail prior art on day one. A single alert is
an array of one. Size cap ~2 KB per alert, ~8 KB per bundle post-encode.
`kind` is an allowlist enum mapping 1:1 onto the
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
All new DB access via `engine/db_conn.py` (see Integration notes). Fired
user-definition alerts emit with Contact Classification tier = ACTIONABLE.

**Phase 2 — Encode / decode (the feature).**
- `engine/alert_share.py`: `encode_alerts(defns) -> str`,
  `decode_alerts(blob) -> defns` (verify sig → version check → schema
  validation → allowlist check → per-alert + bundle size check).
- v1 alerts panel: per-alert "Copy share link" button + multi-select →
  "Copy bundle link" (clipboard write).
- "Paste alert link" box + `POST /api/alert-defs/import` — decodes,
  validates, inserts **all bundle members as disabled** (`enabled=0`); the
  review screen lists every decoded alert with an **"Arm all"** button (plus
  per-alert toggles). One paste = whole bundle recreated, one click = armed —
  TI-parity UX without TI's silent auto-arm, and deliberately *not* TI's
  "erase old settings" semantics — import always creates, never overwrites.
- **Agent auto-emit (§7 decision 2):** once `encode_alerts()` exists,
  agent-generated alerts (e.g. Uhura hits) append their share link to their
  ntfy messages — every notification becomes a TI-style crumb any of the 3
  bridge users can adopt with one paste. Emit-side only; no import-side
  special-casing.
- Duplicate guard: exact (kind, symbol, params) match → offer
  skip/duplicate choice rather than silent double-insert (per bundle member).

**Phase 3 — QR + web landing (DEFERRED per §7 decision 1).**
QR render of the URL wrapper (client-side JS lib, no new backend), landing
page on signal-center showing decoded settings read-only with an Import
button. Audience is the 3 `bridge-allow` emails → clipboard strings are
likely the whole feature; build Phase 3 only if phone-to-desktop sharing
proves annoying in practice. **Still gated on:** verifying/enforcing CF
Access on `signal.ollietrades.com` (existing ⚠ TODO in CLAUDE.md).

**Phase 4 (DEFERRED, pattern B) — short codes.** Server-stored configs with
opaque codes (TI `Cloud.html?code=` style): shorter links, revocation,
share-count. Table `shared_alerts` keyed by 16-byte random code. Same
audience rationale as Phase 3 — not needed for v1 value; ship only if link
length proves annoying to the 3-person audience.

## 5. Security & doctrine compliance

- **RULE #1:** nothing here touches order paths, Schwab, or any execution
  surface. Alert definitions can only feed the existing notify pipeline.
  Import path must be physically unable to create trades — enforced by
  `kind` allowlist mapping only to notify-side checks.
- **No auto-arm on paste:** all imported definitions land `enabled=0` and
  require an explicit human arm action (per-alert or "Arm all" after
  review). A pasted blob silently activating behavior is an injection
  pattern, not a convenience.
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

- Unit: encode→decode round-trip (single + bundle); tamper (flip one byte →
  reject); version bump rejection; oversize rejection (per-alert and bundle);
  per-kind param validation matrix; duplicate-guard per bundle member.
- **Frontend Ship Rule (HM-BJ.E4):** the copy button, paste box, and
  review/Arm-all screen are non-trivial frontend JS — manual browser
  hover/click smoke test is a required closure phase, not optional.
- **Restart-then-verify (HM-CONSOLE-INIT):** smoke-restart trader after the
  `dynamic_alerts.py` union change; confirm log heartbeat shows the
  definition reader firing in the live loop (Daemon Lifecycle Rule — verify
  the live path, not an import-test).
- Rollout order: Phase 1 behind a `config.py` flag
  (`ALERT_DEFS_ENABLED`, default `False`) per Feature Flags convention;
  flip after smoke; Phase 2 ships only after Phase 1 stable ≥1 week.

## 7. Admiral decisions (2026-07-07) — supersedes open questions

1. **Audience: the 3 `bridge-allow` emails only.** Phases 3–4 marked
   DEFERRED/over-build; Phase 2 clipboard strings are the feature.
2. **Agent auto-emit: YES.** Once `encode_alerts()` exists, agent alerts
   (Uhura etc.) append share links to their ntfy messages. Folded into
   Phase 2 scope.
3. **HMAC key: dedicated `ALERT_SHARE_KEY`** env var alongside SMTP/ntfy
   config. Rotation decoupled from signal-center's session secret.
4. **Telegram unification: OUT OF SCOPE.** `dynamic_alerts.py`'s legacy
   `_send_telegram` path vs ntfy-first doctrine to be ticketed separately in
   `docs/XO_BACKLOG.md` (ticket not yet filed as of this update).

## 8. References

- Trade Ideas Collaborate: trade-ideas.com/guide/chapter/9_14/9.14Collaborate.html
- Trade Ideas Cloud share: help.trade-ideas.com/article/91-using-the-cloud-features
- `engine/alert_channels.py` (delivery layer, topics, rate limits)
- `engine/dynamic_alerts.py` (checks + `dynamic_alerts` event table, lines 23–58)
- `engine/db_conn.py` (shared sqlite helper — REQUIRED for all new DB access,
  HM-WAL-BUSY-TIMEOUT-HYGIENE 2026-07-06)
- `_nightcrew/carrier_rung1_contact.html` (Rung 1 contact card — tier wiring)
- `signal-center/server.py` — `/api/export/<fmt>` (~1070), `/api/import` (~1107)
- `dashboard/app.py` — dynamic-alerts endpoints (~8871, ~9261–9289, ~17782)
- CLAUDE.md — TWO-TIER BRIDGE, CARRIER DOCTRINE, Feature Flags, SACRED DATA,
  Frontend Ship Rule, ⚠ signal.ollietrades.com CF Access TODO
