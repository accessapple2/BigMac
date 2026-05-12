# 🔧 SCOTTY — HM-BL-broad: yf_safe Adoption Sweep
### Cluster Adoption · Opus 4.7 · Discover → Diff → Apply

> **Captain's orders, Mr. Scott:** HM-BL shipped engine/yf_safe.py + adopted at engine/high_iv_scanner._get_iv_rank. Your closure listed ~10 remaining yfinance call sites. Sweep them. Single bundle commit. Each swap is `yf.download(...)` / `yf.Ticker(...).history(...)` → `yf_safe.yf_history_safe(...)`. Anchor each `# === HM-BL-broad ===`. Lock in the architectural win.

## Pre-flight

```bash
cd ~/autonomous-trader
git log origin/main --oneline | head -3
git status --short
pgrep -af main.py | head -1
```

## Standing Rules

1. Diff-then-apply per site.
2. Single commit covers full sweep (one revert = full rollback).
3. ntfy each phase.
4. Push + restart + 10-min soak verify inline.
5. HALT after BLB.0 for Captain scope confirmation.
6. venv/bin/python3 for compile + smoke (HM-BHBI lesson).

## Phase BLB.0 — Discovery

```bash
echo "── 1. ALL yfinance call sites ──"
grep -rn "yf\.download\|yf\.Ticker\|\.history(\|yfinance\." engine/ scripts/ shared/ main.py 2>/dev/null | grep -v "yf_safe\|test_\|.pyc" | head -30

echo ""
echo "── 2. Cross-ref BKBL closure follow-up list ──"
grep -A 30 "HM-BL-broad" data/scotty_hm_bkbl_report.md 2>/dev/null | head -40

echo ""
echo "── 3. Each candidate — surrounding context ──"
for FILE_LINE in $(grep -rn "yf\.download\|\.history(" engine/ shared/ 2>/dev/null | grep -v "yf_safe\|test_\|.pyc" | head -15 | awk -F: '{print $1":"$2}'); do
  FILE=$(echo $FILE_LINE | cut -d: -f1)
  LINE=$(echo $FILE_LINE | cut -d: -f2)
  echo ""
  echo "── $FILE_LINE ──"
  START=$((LINE - 2))
  END=$((LINE + 4))
  sed -n "${START},${END}p" $FILE 2>/dev/null
done

echo ""
echo "── 4. yf_safe interface (what we standardize on) ──"
sed -n '1,60p' engine/yf_safe.py 2>/dev/null

echo ""
echo "── 5. Sites that should NOT be swapped? ──"
grep -rn "no_cache\|fresh\|bypass.cache\|nocache" engine/ shared/ 2>/dev/null | head -10
```

Document in `data/scotty_hm_blb_report.md`:
- Table: each site (file:line) + current call + proposed replacement
- Flag exclusions (fresh-fetch intentional, special error handling)
- Q1: scope — all candidates OR exclude N sites for reason X?
- Q2: signature compat — does yf_history_safe(symbol, period, interval) cover all patterns, or need a yf_download_safe companion for bulk fetches?

ntfy. HALT.

## Phase BLB.1 — Apply sweep

For each approved site: diff → apply with anchor → preserve local error handling.

Compile + smoke (live ticker SPY + delisted ATH through wrapper).

Commit: `refactor(yfinance): HM-BL-broad — adopt yf_safe memoization wrapper at remaining N call sites`. ntfy.

## Phase BLB.C — Verify

```bash
echo "── Anchors ──"
grep -rn "HM-BL-broad" engine/ shared/ 2>/dev/null | head -15

echo ""
echo "── Compile via launchd interp ──"
venv/bin/python3 -c "
import py_compile, os
files = [f for f in os.popen('grep -rln \"HM-BL-broad\" engine/ shared/').read().strip().split() if f]
for f in files:
    py_compile.compile(f, doraise=True)
    print(f'  {f}: clean')
"

echo ""
echo "── Any remaining raw yfinance calls outside yf_safe.py / intentional exclusions ──"
grep -rn "yf\.download\|\.history(" engine/ shared/ 2>/dev/null | grep -v "yf_safe\|test_\|.pyc" | head -10
```

ntfy.

## Phase BLB.D — Closure + push + restart + 10-min soak (INLINE)

```bash
git push origin main
launchctl kickstart -k gui/$(id -u)/com.trademinds.trader
sleep 30
NEW_PID=$(launchctl list | grep com.trademinds.trader | awk '{print $1}')
echo "  New PID: $NEW_PID"
echo "  Port :8080:"; lsof -ti :8080 | head -1

BASELINE=$(grep -c '\$ATH' ~/autonomous-trader/logs/trader_error.log)
echo "  T+0 \$ATH count: $BASELINE"

echo ""
echo "── 10-min soak ──"
sleep 600

POST=$(grep -c '\$ATH' ~/autonomous-trader/logs/trader_error.log)
DELTA=$((POST - BASELINE))
echo "  T+10min: $POST"
echo "  Delta over soak: +$DELTA"
if [ "$DELTA" -lt 5 ]; then
  echo "  ✅ HM-BL-broad working — all emitters routing through memoize"
else
  echo "  ⚠️  +$DELTA still leaking — re-inspect:"
  grep '\$ATH' ~/autonomous-trader/logs/trader_error.log | tail -5
fi
```

Append closure with the delta. ntfy: `🏁 HM-BL-broad complete`.
