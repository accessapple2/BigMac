# 🔧 SCOTTY — HM-BD.H: scanned_at Cache Mystery Trace
### Opus 4.7 · Single-phase trace + fix · Auto-mode

> **Captain's orders, Mr. Scott:** Resolve the deferred-from-MONSTER-1 cache mystery. Symptom: `scan_premarket_gaps()` direct Python call returns ISO format datetime correctly, but `/api/premarket-gaps` endpoint returns SPACE format datetime that PREDATES the last process restart. In-memory dicts can't survive restart, so the data source isn't what we assumed.

## Pre-flight + BDH.0 discovery + BDH.1 apply (if obvious) + BDH.C verify
