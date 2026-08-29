# Relay — 2026-07-19 (later) — DECOM travel copy served + MANIFEST §11 added

## What shipped

**Travel copy built + served for Bonnie's PC:** `~/DECOM_STAGING_TRAVEL`
(~161MB), a reduced copy of `~/DECOM_STAGING` (the DECOM-MASTER Gate 1
decommission archive). `secrets/` excluded entirely except the 3 redacted
placeholder files (`env-root.EXAMPLE-REDACTED.env`, `env-root.example`,
`env-swingdesk.EXAMPLE-REDACTED.env`); the 4 live-key files are absent
from the travel copy. Served via `python3 -m http.server 8899` on bigmac,
LAN-only: `http://192.168.1.248:8899/`. Original `~/DECOM_STAGING`
untouched.

**MANIFEST.txt located and updated:** `~/DECOM_STAGING/MANIFEST.txt` is
the canonical MANIFEST referenced in yesterday's offsite-backup custody
naming conversation — the salvage/backup thread and the DECOM-MASTER
decommission archive are the same effort; earlier same-day search just
didn't find it. Added new §11 STORAGE & CUSTODY recording all three
drives:
- Data (X9, CT1000X9SSD9, UUID `76C5-99E2`) — resident at bigmac.
- B-4 (WD My Passport, offsite safe) — verified byte-identical twin
  (327,616/327,616 files, 20/20 SHA256 sample).
- TRAVELER (WD Elements, SN `WX91AA47C8CD`) — travel copy for external
  analysis, no personal data, no live keys.

Change applied to the source MANIFEST.txt (444 lines total after) and
copied into the served `DECOM_STAGING_TRAVEL/MANIFEST.txt` so the two
stay in sync; confirmed via `curl` against the live HTTP server that the
served copy matches byte-for-byte (`diff` clean).

## What's open

- Standing orders unchanged: hold at anchor, no Part B, no Gate 2.
- HTTP server (PID logged at start time, port 8899) is still running in
  the background on bigmac — stop it once Bonnie's PC has pulled the
  travel copy; it's LAN-only but there's no reason to leave it up
  indefinitely.
- `~/DECOM_STAGING_TRAVEL` is a point-in-time copy — if `DECOM_STAGING`
  changes again (e.g. another MANIFEST edit), re-sync the travel copy
  before re-serving.
