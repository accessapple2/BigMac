# Relay — 2026-07-19 — Offsite backup verified, B-4 custody naming

## Status update (acknowledged)

Data is back on bigmac. Spotlight (`mdutil`) is re-indexing it — confirmed
in progress, no suppression, letting it run.

**Second copy verified byte-identical:** WD My Passport (E:, NTFS) now holds
a full verified copy of everything on Data — 327,616 files matched, 20/20
SHA256 sample passed. Proof archived at `E:\Transfer 2026\VERIFICATION\` on
Bonnie's PC (work performed by a Claude Code session there, not this one).
Salvage + personal files are double-covered.

**Standing orders unchanged:** hold at anchor. No Part B, no Gate 2, fleet as-is.

**Resume note for Part B (not yet started):** before Part B begins, check
`mdutil -s` on the X9. If first-pass indexing has completed and `mds` is
idle, proceed normally. If still storming, use the daemon-level disable
discussed previously (not detailed here — pull from the original
conversation when Part B actually starts).

## Naming update — MANIFEST custody section

Per Captain's instruction, for the record:

- **B-4** = WD My Passport 0820 (NTFS), the offsite second-copy drive.
  Holds the verified byte-identical twin of Data's full contents
  (327,616 files, hash-verified). Stored physically separate from Data.
- **Storage & Custody** (both drives):
  - **Data** — X9, CT1000X9SSD9, UUID `76C5-99E2` — resident at bigmac.
  - **B-4** (My Passport) — offsite.

**Note:** the canonical MANIFEST document itself was not found in this
repo (`autonomous-trader`) or searchable locally on bigmac as of this
session — it likely lives with the salvage/migration project on the Data
volume or on Bonnie's PC where the verification work ran. This relay entry
records the naming decision and custody facts as instructed; the actual
MANIFEST file's Storage & Custody section should be updated with this
content wherever that document lives. Flagging rather than guessing a path.

No other action taken. Holding at anchor.
