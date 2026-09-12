# Relay — 2026-09-12. Bridge Correctness Pass, Phase 2 (safe display fixes).

RULE #1 respected: every fix here is display/derivation code. No trade or
trader history row was deleted, rewritten, or touched. All 5 changed files
are frontend HTML/JS or read-path Python (dashboard rendering, a
notification-text builder, a display-cache fallback chain) — nothing in
the trading decision path was modified.

Files touched: `dashboard/app.py`, `dashboard/static/bridge-v2.html`,
`dashboard/static/index.html`, `engine/archer_morning_synthesis.py`,
`engine/premarket_scanner.py`. `py_compile` clean on all three Python
files; `node --check` clean on both HTML files' full inline-script
content. Two of the trickier fixes (5, 2b) verified live in the browser
against the running trader (bridge.ollietrades.com); the rest verified by
calling the underlying live functions/endpoints directly and inspecting
real data, per "Verify before claiming."

## The 8 originally-listed items

**5. Shadow CSP bake-off DSR shown with N=3** — `bridge-v2.html`'s
`loadBakeoffStandings()` now withholds the numeric DSR below
`scripts/archive_harness.py`'s own `MIN_N_DEFAULT=5`, showing `— (N=X)`
instead. **Verified live in-browser**: `shadow-qwen35-csp` (n_closed=3)
now shows `— (N=3)` where it previously showed `0.988`.

**6. Kirk Advisory "retired path"** — **investigated, no fix — the
premise was wrong.** `data/real_holdings.json` is not retired; it's the
live canonical file `scripts/sync_schwab_live.py` writes every 15 min
during market hours (confirmed fresh: `last_updated: 2026-09-11`, correct
for a closed Saturday). What retired 2026-06-14 was the manual CSV-import
path. `engine/kirk_advisory.py::_load_real_holdings()` already has an
explicit weekend-aware staleness guard. Bridge-v2's Kirk widget is
honestly labeled "paper-only" and correctly reads paper, not Schwab.
Forcing a change here would have been fixing something that wasn't broken.

**7. Hardcoded "Season 5"** — bridge-v2.html was already dynamic (fixed
2026-08-29, HM-BRIDGE-SEASON-PNL). Found and fixed 3 real hardcodes:
`index.html`'s Fleet P&L glance row (now reads `resp.current_season` from
the same `/api/arena/leaderboard` fetch it already makes), `dashboard/app.py`'s
`/api/v1/docs` footer (now calls `season_manager.get_current_season()`),
and `_build_computer_context()`'s Archer-chat context builder (now reads
`current_season` off the cached leaderboard it already fetches).

**8. Leaderboard rank skip + wrong sort** — **rank skip: found and fixed.**
`index.html`'s `buildArenaLbRows()` (the shared row-builder behind the
main Fleet/Arena leaderboard) skipped benched players (`continue`) without
decoupling the displayed rank from the raw loop index, so a benched
player's array position left a gap in the rank sequence (e.g. position 2
skipped meant rank 3 was never shown). Fixed with a separate
post-filter counter. **Sort-direction claim: investigated, could not
reproduce.** Called the live backend (`leaderboard()`) directly — Dilithium
Reserve (`enterprise-computer`, -7.02% return) is correctly last (rank 11
of 11) in the raw, already-sorted response; `leaderboard.html`'s own
client-side sort is also correctly descending. Checked three separate
render paths (bridge-v2 x2, `leaderboard.html`) and found no code path
that sorts on one field while displaying another. Flagging as unverified
rather than fabricating a fix — if it recurs, a screenshot with the sort
button state would pin it down.

**9. Agent counts disagree 4 ways** — **investigated, documented, not
force-fixed.** Live-checked the arithmetic: Systems Status's "8 active ·
74 halted" sums to 82, which matches `ai_players`' real live total exactly
(`SELECT COUNT(*)` = 82 = 8 active + 3 exit_only + 71 full). Internally
correct. The "11 Agents" leaderboard figure is a deliberately curated
named-crew subset (`/api/arena/leaderboard`'s own smaller roster), not
meant to equal the full `ai_players` count — a definitional difference,
not a bug. This is precisely the class of problem Phase 3's proposed
`/api/bridge/facts` endpoint exists to solve; patching individual widgets
to agree on an ad hoc basis would just move the inconsistency around.

**10. Archer briefing NUKZ x3 + mangled character** — both root-caused and
fixed in `engine/archer_morning_synthesis.py`. NUKZ x3: `get_uhura_signals()`
had no per-ticker dedup, so multiple `institutional_signals` rows for the
same ticker within the lookback window all made it into the briefing —
now deduped, keeping the most recent row per ticker. Mangled character:
`build_briefing()` ran `.encode("ascii", errors="replace").decode("ascii")`
on the entire body, turning every em-dash into a literal `?` (confirmed:
`f"{ticker}: {signal} — {reason}"`'s `—` is exactly what broke). The
ntfy title this was meant to protect is a separate hardcoded string
(`"Morning Briefing -- Archer"`, never built from this text), and
`engine.alert_channels._send_ntfy()` already ASCII-safes its own title
independently while sending the message body as full UTF-8 — the strip
was protecting against a problem that didn't exist at this call site,
while corrupting real punctuation. Removed.

**11. Consensus panel ~300 empty-vote tickers** — `index.html`'s XO Room
consensus panel (`/api/consensus`, `cd.tickers`) rendered every key with
no filter. Live-checked `build_consensus()`: 316 tickers, 301 with zero
votes from all three of Spock/Data/Uhura. Now shows only tickers with at
least one real vote, with an "+N more tickers scanned with no crew votes
yet" line for the rest.

**12. Metals panel self-contradiction** — root cause: a field-name
mismatch. `/api/metals/portfolio`'s real per-holding array is named
`positions` (confirmed live: 2 real rows, GC=F/SI=F, with real
`unrealized_pnl_pct`); the glance-row JS checked `d.holdings`, which
doesn't exist on the response, so it always fell to "No metals data."
regardless of real data being present one field over — the header right
above it was reading `d.return_pct` from the very same response
correctly the whole time. Fixed the field name and the per-position field
mapping. Also added a tooltip clarifying the header's -15.92% is
unrealized P&L vs. cost basis, a different (and legitimately different)
number from the separate Metals Exposure panel's live spot-price daily
change (+0.04%) — both numbers are real, they're just different metrics
with no distinguishing label.

## 3 additional fixes folded in from the Phase 1 trace

**2b. Battle Station "Recent history" mislabeled as today's trades** —
added a "Recent history (any date)" label above `bsHistory` on
bridge-v2.html. **Verified live in-browser**: two real trades from
2026-06-08 and 2026-04-27 now display correctly labeled, instead of
sitting unlabeled directly under "Trades today."

**3. Sector heatmap zero-fill** — `engine/premarket_scanner.py`'s
`get_sector_heatmap()` now discards a Finviz response only when every
sector in it reads exactly 0.0 (the confirmed live Saturday behavior —
Finviz genuinely zeros out all 11 sectors with market closed), letting the
existing Yahoo-then-stale-disk fallback chain actually run. **Verified
live**: before the fix, all 11 Finviz-covered sectors read 0.0; after,
6 of 12 sectors show real distinct Yahoo values (Utilities +0.14%,
Real Estate +0.10%, Energy +0.09%, Financials +0.04%, Technology -0.01%,
Industrials +0.01%) and the disk cache — incidentally polluted with
zeros by this session's own earlier live-testing before the fix landed —
self-healed on the same call.

**4. Riker mid-sentence display bug** — `index.html`'s
`fetchRikerRecommendation()` used to silently delete `<unusedNNNN>`
tokenizer-leakage artifacts (empty-string replace). When the artifact
lands where the model's verdict word should be, the sentence loses its
first word with no trace. Now replaces with a visible `[?] ` marker
instead — a dropped word now reads as dropped.

## Restart + live verification (2026-09-12 07:34 MST)

Backup-first (`db_snapshot.sh`), `trader_restart.sh` clean restart (PID
74944 -> 88836, started 07:34:25, 7s after commit `3ab0b14` at 07:34:18 —
the running process reflects this pass). Verified from the running
process, not the file: `curl http://127.0.0.1:8080/api/v1/docs` (a public
endpoint) now returns `Season 8 · Phase 3.2 Public API` where it used to
say `Season 5`. Post-restart `trader_error.log`: clean startup, `ALL
SYSTEMS OPERATIONAL — ENGAGE`, no new errors.

## What's still open

- Item 8's sort-direction claim (Dilithium Reserve ranking first) —
  unreproduced against live data across all three render paths checked.
- Item 9 — full cross-panel agent-count unification is Phase 3's job, not
  patched here.
- Item 6 — nothing to fix; documented why.
