# 🔧 HM-BD.F-audit — Silent-Pass Inventory in ai_brain.py (HALT FOR CAPTAIN)

**Author:** Scotty (Opus 4.7)
**Date:** 2026-05-12
**Status:** HALT — Captain scope decision needed for HM-BD.F2 follow-up
**Prior art:** HM-BD.F (commits at L948 + L972) applied loud-fail to two HTTP-fetch sites in `_collect_market_data`. The remaining `except Exception: pass` sites haven't been touched.

---

## Inventory

**Total bare silent-pass `except Exception:` blocks in `engine/ai_brain.py`: 23** (directive estimated 22 — close enough; the gap is one shared `try/except` chain that read as two).

Sites already converted by HM-BD.F (L948, L972 — narrow-strict on requests/Timeout/Connection): excluded from this audit since they're done.

### Classification (auto-classified + manually verified)

| # | Line | Auto | Verified | Site description |
|---|---|---|---|---|
| 1  | L275  | DB_WRITE | DB read (pause_all setting) | Settings table read; falls through if missing |
| 2  | L290  | NETWORK | INTERNAL (cache fetch) | `get_cached_discoveries()` — local cache read |
| 3  | L518  | INTERNAL_STATE | INTERNAL | `_bc_invalidate()` cache flush |
| 4  | L539  | DB_WRITE | INTERNAL | Pre-scan eligibility check |
| 5  | **L580** | **NETWORK** | **NETWORK** ✓ | Ollama `keep_alive=0` POST — model unload |
| 6  | L613  | INTERNAL_STATE | INTERNAL | `tier1_has_signal` recent-signal lookup |
| 7  | **L711** | INTERNAL_FETCH | **DB_WRITE** ✓ | `record_portfolio_snapshot("alpaca-mirror", prices)` |
| 8  | L746  | INTERNAL_STATE | INTERNAL | PAIR TRADE display log fallback |
| 9  | L773  | INTERNAL_STATE | DB write (ghost outcomes) | `update_ghost_outcomes(prices)` — DB write but ghost-only |
| 10 | L805  | INTERNAL_FETCH | INTERNAL (cache fetch) | `_get_vix_cached()` |
| 11 | L832  | DB_WRITE | INTERNAL | `is_auto_tradeable` DB-backed check |
| 12 | L844  | DB_WRITE | INTERNAL | `is_human_player(player_id)` guard |
| 13 | L858  | INTERNAL_STATE | INTERNAL | Earnings-day gate |
| 14 | L927  | INTERNAL_STATE | INTERNAL | Memory injection into scan_ctx |
| 15 | **L1029** | INTERNAL_STATE | **DB_WRITE** ✓ | `record_signal(player_id, …)` — signal_tracker insert |
| 16 | L1038 | INTERNAL_STATE | DB write | `record_trade_signal` insert |
| 17 | L1062 | INTERNAL_STATE | DB write | `_record_trail_signal` insert |
| 18 | L1246 | INTERNAL_STATE | INTERNAL | `_flow_confirms` flag detection |
| 19 | L1253 | INTERNAL_STATE | INTERNAL | `_has_catalyst` earnings detection |
| 20 | L1268 | INTERNAL_STATE | INTERNAL | `_has_catalyst` news detection |
| 21 | L1324 | INTERNAL_STATE | INTERNAL | VIX sizing modifier |
| 22 | **L1406** | **NETWORK** | **NETWORK** ✓ | `requests.post("http://localhost:9000/api/signal", …)` |
| 23 | L1409 | NETWORK | INTERNAL (thread spawn) | Outer catch around `Thread(daemon=True).start()` |

Bolded rows are HM-BD.F2 candidates. Bold ticks indicate manually verified.

### Summary by category

| Category | Count | Loud-fail policy per CLAUDE.md error-handling posture |
|---|---|---|
| **True HTTP/network** (L580, L1406) | 2 | Loud-fail recommended — type+repr at minimum |
| **DB-write to observability tables** (L711, L1029, L1038, L1062) | 4 | Loud-fail recommended — silent failure = data gap in benchmark/signal/trail tables |
| **DB-write to ghost-only tables** (L773) | 1 | Optional — ghost data is research-only, not load-bearing |
| **DB-read for control-flow** (L275, L539, L832, L844) | 4 | Keep silent — fallback paths are intentional behavior |
| **Internal cache/state fallbacks** | 12 | Keep silent — per CLAUDE.md "per-agent cycles where one agent's crash shouldn't take down the fleet" |
| Total | 23 | |

---

## Recommended HM-BD.F2 scope

### Tier 1 — RECOMMENDED (4 sites, network + observability DB writes)

| Site | Treatment |
|---|---|
| **L580** Ollama unload POST | Narrow-strict catch `(requests.RequestException, TimeoutError, ConnectionError)`, log `[yellow]Ollama unload {model_id}: {type(e).__name__}: {e!r}` |
| **L1406** signal-center POST | Same narrow-strict + log `[yellow]signal-center post failed: {type(e).__name__}: {e!r}` (running inside a daemon thread, log only — no NTFY since signal-center being down is operational, not architecture-class) |
| **L711** record_portfolio_snapshot | Narrow-strict on `(sqlite3.Error, KeyError, ValueError)`, log `[yellow]alpaca-mirror snapshot failed: {type(e).__name__}: {e!r}`. Silent failure = equity-curve gap which is what Item-3 was meant to fix. |
| **L1029** record_signal | Same pattern, log `[yellow]record_signal {player_id} {symbol}: {type(e).__name__}: {e!r}`. Silent failure = signal-tracker gap. |

**Estimated diff: ~16 lines (4 sites × 4-line replacement)**, all within `engine/ai_brain.py`, mirrors HM-BD.F's existing pattern at L948/L972 verbatim.

### Tier 2 — OPTIONAL (2 additional sites)

| Site | Treatment | Rationale |
|---|---|---|
| L1038 record_trade_signal | Match Tier 1 pattern | Same shape as L1029, just different function |
| L1062 _record_trail_signal | Match Tier 1 pattern | Same shape as L1029, just different function |

### Tier 3 — KEEP SILENT (17 sites, NOT recommended)

The remaining 17 sites are internal-state fallbacks where silent-pass is the intended behavior per CLAUDE.md error-handling posture principle 1: *"Bare `except Exception` is acceptable when the handling correctly accommodates unknown failures."* Examples:

- L1296 falls back to a default Dalio symbol set
- L1181 falls back to `asset_type="stock"`
- L518/L832/L844 are guard-gates where failure should NOT crash the per-agent cycle

Touching these would add log noise without observational value.

---

## Questions for Captain

**Q1 — Scope:**
- (A) **Tier 1 only** (4 sites, ~16-line diff) — **recommended**
- (B) Tier 1 + Tier 2 (6 sites)
- (C) Custom cherry-pick (specify which sites)

**Q2 — Pattern alignment:**
- Mirror HM-BD.F's `except (requests.RequestException, TimeoutError, ConnectionError, KeyError, ValueError) as e:` shape verbatim, OR pick exception classes per site? (HM-BD.F's broad list works for everything except L711/L1029 which need `sqlite3.Error` in place of `requests.RequestException`.)

**Q3 — NTFY threshold:**
- Per CLAUDE.md posture: NTFY is reserved for architecture-class paths (broker-submit, halt_mode writes, position-of-record). None of these 4 Tier-1 sites qualify (closest is L711 which writes to `portfolio_history`, a benchmark/equity-curve table — not position-of-record). **Recommend log-only, no NTFY.** Confirm?

---

## Closure status

Discovery only. No code changes in this commit. Captain decision on Q1/Q2/Q3 unlocks the implementation step (HM-BD.F2 epic, separate session).
