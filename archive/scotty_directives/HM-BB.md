# 🔧 SCOTTY — HM-BB: Ghost Trader Schema Enrichment
### Schema Migration · Opus 4.7 · Discover → Migrate → Wire → Verify

> **Captain's orders, Mr. Scott:** HM-AZ shipped `trader.db.ghost_trades` as canonical. The reader and writer now agree on the table, but the schema is thin — missing `entry_price`, `confidence`, `exit_price`, `pnl_pct`. Add those columns, wire `scripts/ghost_advisor.py` to populate them on INSERT, wire `engine/ghost_trader.py` to surface them on read. Sacred-DB schema migration — backup first, ALTER TABLE second, idempotent guards required.

---

## Identity & Mission

You are **Scotty** — Claude Code on Opus 4.7. Single epic, four atomic phases.

Mission:
- **BB.0** — Discovery: inventory current schema, writer, updater, reader locations. NO writes.
- **BB.1** — Backup `trader.db` snapshot before any sacred-DB write.
- **BB.2** — `ALTER TABLE ghost_trades ADD COLUMN` for `entry_price`, `confidence`, `exit_price`, `pnl_pct` — each guarded by an existence check (SQLite has no `ADD COLUMN IF NOT EXISTS`, so check `PRAGMA table_info` first).
- **BB.3** — Update `scripts/ghost_advisor.py` INSERT (open) and any update statements (close) to populate the new columns. `pnl_pct = (exit_price - entry_price) / entry_price * 100` on close.
- **BB.4** — Update `engine/ghost_trader.py` (post HM-AZ) to surface the new columns in `get_stats()` / any reader endpoint that the dashboard hits.

---

## Pre-flight check

```bash
cd ~/autonomous-trader

echo "── Verify HM-AZ shipped to origin ──"
git log origin/main --oneline | grep -iE "HM-AZ|ghost.*query|ghost.*rewrite" | head -5

echo ""
echo "── Verify service is on current code (PID stable) ──"
pgrep -af main.py | head -1

echo ""
echo "── Current trader.db.ghost_trades schema ──"
sqlite3 ~/autonomous-trader/data/trader.db ".schema ghost_trades"

echo ""
echo "── Current row count ──"
sqlite3 ~/autonomous-trader/data/trader.db "SELECT COUNT(*) FROM ghost_trades;"

echo ""
echo "── Sample rows (most recent 3) ──"
sqlite3 -header -column ~/autonomous-trader/data/trader.db "SELECT * FROM ghost_trades ORDER BY rowid DESC LIMIT 3;"
```

If HM-AZ commits don't show in origin: **HALT**. HM-BB requires HM-AZ.

---

## Standing Rules

1. Sacred DBs: read + WRITE per this directive. Schema migrations are explicit, idempotent, backed up.
2. Sacred directories: no `rm -rf`.
3. Diff-then-apply: unified diff before every code edit. SQL diff via `.schema` before/after.
4. One commit per sub-phase. Each commit independently revertable.
5. NTFY on commit: `curl -d "✅ HM-BB.X: <one-line>" https://ntfy.sh/ollietrades-admin`.
6. Push gate: do NOT push. Stage commits locally — Captain pushes after verify.
7. NO service restart. Captain handles it.
8. HALT after each phase and report — Captain confirms before next phase.

---

## Phase BB.0 — Discovery (NO writes)

Inventory current state so BB.1–BB.4 are precise:

```bash
cd ~/autonomous-trader

echo "── 1. Existing columns ──"
sqlite3 ~/autonomous-trader/data/trader.db "PRAGMA table_info(ghost_trades);"

echo ""
echo "── 2. Writer locations (INSERTs into ghost_trades) ──"
grep -rn "INSERT INTO ghost_trades\|INSERT.*ghost_trades" scripts/ engine/ 2>/dev/null | head -10

echo ""
echo "── 3. Updater locations (UPDATEs on ghost_trades — close/exit hooks) ──"
grep -rn "UPDATE ghost_trades\|mark_closed\|update_ghost\|ghost.*close" scripts/ engine/ 2>/dev/null | head -10

echo ""
echo "── 4. Reader locations (SELECTs on ghost_trades) ──"
grep -rn "FROM ghost_trades\|ghost_trades " scripts/ engine/ dashboard/ 2>/dev/null | head -15

echo ""
echo "── 5. Confidence field in scope of ghost_advisor (likely from signal dict) ──"
grep -n "confidence\|conf\b" scripts/ghost_advisor.py 2>/dev/null | head -10
```

Produce a report `data/scotty_hm_bb_report.md` with:

```markdown
# HM-BB Discovery

## Existing columns
<paste PRAGMA output>

## Missing columns
<list of: entry_price, confidence, exit_price, pnl_pct that are NOT already present>

## Writer file
scripts/ghost_advisor.py — INSERT at line N (single statement / multiple)
Variables in scope at INSERT: entry_price=?, confidence=?

## Close hook
scripts/ghost_advisor.py line N — UPDATE statement (yes/no)
OR no close-side write yet — must add one

## Reader file
engine/ghost_trader.py — function get_stats() at line N
Dashboard endpoint that hits it: <path>

## Safe to insert?
Yes/No based on existing-column overlap

## Restart impact
Yes — schema change requires service restart to pick up new column reads/writes.
```

**HALT.** ntfy: `📋 HM-BB discovery complete`.

---

## Phase BB.1 — Backup snapshot

```bash
cd ~/autonomous-trader

TS=$(date +%Y%m%d_%H%M)
cp data/trader.db "data/trader.db.pre-hm-bb-${TS}"

ls -la data/trader.db.pre-hm-bb-* | tail -3
sqlite3 "data/trader.db.pre-hm-bb-${TS}" "SELECT 'backup ok', COUNT(*) FROM ghost_trades;"
```

If backup row count matches live: proceed. If mismatch: HALT, ntfy admin.

ntfy: `📦 HM-BB.1: trader.db backed up (<TS>)`.

---

## Phase BB.2 — Schema migration

Run ONLY columns that are missing per Phase 0 discovery. Each ALTER guarded:

```bash
cd ~/autonomous-trader

python3 << 'EOF'
import sqlite3
DB = "data/trader.db"
NEW_COLS = [
    ("entry_price", "REAL"),
    ("confidence",  "REAL"),
    ("exit_price",  "REAL"),
    ("pnl_pct",     "REAL"),
]

conn = sqlite3.connect(DB)
cur = conn.cursor()

existing = {row[1] for row in cur.execute("PRAGMA table_info(ghost_trades)").fetchall()}
print(f"Existing columns: {sorted(existing)}")

added = []
for col, typ in NEW_COLS:
    if col in existing:
        print(f"  SKIP {col} — already present")
        continue
    sql = f"ALTER TABLE ghost_trades ADD COLUMN {col} {typ}"
    print(f"  RUN  {sql}")
    cur.execute(sql)
    added.append(col)

conn.commit()
conn.close()
print(f"\nAdded: {added or 'nothing (all columns already present)'}")
EOF

echo ""
echo "── Post-migration schema ──"
sqlite3 ~/autonomous-trader/data/trader.db ".schema ghost_trades"

echo ""
echo "── Row count unchanged ──"
sqlite3 ~/autonomous-trader/data/trader.db "SELECT COUNT(*) FROM ghost_trades;"

echo ""
echo "── New columns are NULL for existing rows (expected) ──"
sqlite3 -header -column ~/autonomous-trader/data/trader.db "SELECT rowid, entry_price, confidence, exit_price, pnl_pct FROM ghost_trades ORDER BY rowid DESC LIMIT 3;"
```

If existing rows show NULL for new columns: correct. If schema doesn't show the new columns at all: HALT.

Commit: there's no code change to commit for BB.2 — schema lives in the DB itself. Tag this phase in `data/scotty_hm_bb_report.md` as "DB-migration only, no commit."

ntfy: `🗄️ HM-BB.2: schema migrated (4 columns added or skipped per idempotent guard)`.

---

## Phase BB.3 — Wire writer (scripts/ghost_advisor.py)

Locate the INSERT identified in Phase 0. Two cases:

### Case A: Single INSERT covers open
Add the new columns to the INSERT column list AND values list. Map from existing local variables:
- `entry_price` ← the price at trade open (likely already in scope as `price`, `entry`, or similar)
- `confidence` ← from signal confidence (likely in scope from the signal dict)
- `exit_price` ← NULL on open (gets populated on close)
- `pnl_pct` ← NULL on open

### Case B: INSERT-only writer (no close-side UPDATE)
Add an UPDATE statement to populate `exit_price` and `pnl_pct` when the position closes. This may require finding the close hook (search `mark_closed`, `update_ghost`, or similar).

Show diff. Use `# === HM-BB.3 ===` anchors around new logic.

Compile check:
```bash
python3 -c "import py_compile; py_compile.compile('scripts/ghost_advisor.py', doraise=True); print('clean')"
```

Commit: `feat(ghost): HM-BB.3 — populate entry_price/confidence/exit_price/pnl_pct in ghost_advisor`.

ntfy: `✅ HM-BB.3: writer wired`.

---

## Phase BB.4 — Wire reader (engine/ghost_trader.py)

Locate the reader functions (`get_stats`, `closed_trades`, or similar from Phase 0). Update SELECT column lists to include the new fields.

If the reader returns a dict per row, ensure the dict keys match what the dashboard expects. Per HM-AZ Option B context: trader.db's natural names are `exit_price` and `pnl_pct` — surface those, no aliasing needed.

If the dashboard SQL expected legacy `outcome_price` / `outcome_pnl_pct` aliases (the HM-AZ rewrite handled this), keep that aliasing consistent.

Show diff. Use `# === HM-BB.4 ===` anchors.

Compile check.

Commit: `feat(ghost): HM-BB.4 — surface new columns in ghost_trader reader`.

ntfy: `✅ HM-BB.4: reader wired`.

---

## Phase BB.C — Static verify

```bash
cd ~/autonomous-trader

echo "── HM-BB anchors present ──"
grep -rn "HM-BB" scripts/ engine/ 2>/dev/null | head -10

echo ""
echo "── Both files compile clean ──"
python3 -c "
import py_compile
for f in ['scripts/ghost_advisor.py', 'engine/ghost_trader.py']:
    py_compile.compile(f, doraise=True)
    print(f'  {f}: clean')
"

echo ""
echo "── Schema reflects all 4 new columns ──"
sqlite3 ~/autonomous-trader/data/trader.db "PRAGMA table_info(ghost_trades);" | grep -E "entry_price|confidence|exit_price|pnl_pct"

echo ""
echo "── Smoke: reader can SELECT new columns without error ──"
python3 -c "
import sys; sys.path.insert(0, '.')
import sqlite3
conn = sqlite3.connect('data/trader.db')
cur = conn.cursor()
rows = cur.execute('SELECT rowid, entry_price, confidence, exit_price, pnl_pct FROM ghost_trades LIMIT 1').fetchall()
print(f'Reader can SELECT new columns: {rows}')
conn.close()
"

echo ""
echo "── Commits staged ──"
git log origin/main..HEAD --oneline
```

ntfy: `✅ HM-BB verify clean`.

---

## Phase BB.D — Closure report

Append to `data/scotty_hm_bb_report.md`:

```markdown
## HM-BB Closure

### Commits staged (not pushed)
<output of: git log origin/main..HEAD --oneline>

### Schema diff
Before: <columns from BB.0>
After:  <columns from BB.2 post-migration>
Added: <list>

### Smoke test result
<output of reader SELECT new columns>

### Restart needed
Yes — Captain will run `launchctl kickstart -k gui/$(id -u)/com.trademinds.trader` after push.

### Out-of-scope follow-ups
- Backfill of NULL entry_price/confidence on existing 16 rows (HM-BB.E candidate)
- Dashboard surface of new fields (HM-BA scope)
```

ntfy: `🏁 HM-BB complete — ready for Captain push & restart`.
