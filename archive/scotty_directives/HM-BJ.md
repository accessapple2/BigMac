# 🔧 SCOTTY — HM-BJ: Ticker Linkification + Hover Scorecard
### Dashboard Frontend Sweep · Opus 4.7 · Discover → Tier → Apply

> **Captain's orders, Mr. Scott:** Every ticker symbol across the dashboard should be interactive. Click = internal focus mode (refocus panels on that symbol), shift-click = external TradingView chart, hover = quick-glance scorecard with sparkline + signal/position/ghost detail. Build on existing data — sentiment scores, signals, ghost trades, fleet activity, IV, earnings. Frontend-only epic; no service restart required, but Vite rebuild likely.

---

## Identity & Mission

You are **Scotty** — Claude Code on Opus 4.7. Frontend-heavy multi-phase epic.

Mission:
- **BJ.0** — Discovery: frontend stack, ticker location inventory, available per-symbol data, feasibility tiers.
- **BJ.1** — TickerChip component: hover scorecard with sparkline + key metrics.
- **BJ.2** — Click handlers: internal focus, shift-click → TradingView, optional right-click → context menu (Yahoo/Webull/Schwab/X).
- **BJ.3** — Cross-panel sweep: apply TickerChip everywhere tickers appear.
- **BJ.C** — Static verify + smoke (load dashboard, hover a few tickers, click, shift-click).
- **BJ.D** — Closure + Vite rebuild + git push + browser-side verification. NO service restart needed.

---

## Pre-flight

```bash
cd ~/autonomous-trader

echo "── Prerequisites ──"
git log origin/main --oneline | grep -iE "HM-BD\.F|HM-BDGE" | head -3

echo ""
echo "── Frontend stack detection ──"
ls -la dashboard/frontend/ 2>/dev/null | head -10
cat dashboard/frontend/package.json 2>/dev/null | python3 -c "import sys, json; p=json.load(sys.stdin); print('framework:', list(p.get('dependencies',{}).keys())[:10])" 2>/dev/null
ls dashboard/frontend/src 2>/dev/null | head -20

echo ""
echo "── Working tree clean ──"
git status --short
```

---

## Standing Rules

1. Frontend-only — backend untouched unless an endpoint MUST be added for scorecard data.
2. Diff-then-apply for every file edit.
3. One commit per sub-phase.
4. NTFY each phase.
5. NEW WORKFLOW: git push + Vite rebuild INLINE in BJ.D. NO service restart (frontend assets are served by main.py which doesn't need to reload them — though confirm in BJ.0).
6. HALT after BJ.0 for Captain scope freeze. HALT after BJ.1 for component review.

---

## Phase BJ.0 — Discovery (deep) ... [truncated for brevity in inline copy; full content in original directive]
