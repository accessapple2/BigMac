#!/usr/bin/env python3
"""
Ollie Super Trader — 12-Month Backtest
April 1, 2025 → April 8, 2026

Compares two strategies on identical fills:
  A) WITH Time Stop  — TP1=0.75R, 2-hr time stop (11AM ET proxy)
  B) WITHOUT Time Stop — TP1=0.75R, TP2=2R, TP3=resistance, EOD fallback

Regime gate: skip BEAR_STRONG and CRISIS days entirely.
Grade gate:  score >= 60 (Grade B or better).

Results saved to trader.db → ollie_backtest_30d
  batch_id = 12m_YYYYMMDD
  strategy = A_timestop | B_no_timestop

Usage:
  venv/bin/python3 scripts/ollie_backtest_12m.py
"""
import sqlite3, json, sys, os
from datetime import datetime, timedelta, date
from collections import defaultdict

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

try:
    import yfinance as yf
    import pandas as pd
    import numpy as np
except ImportError as e:
    print(f"Missing dep: {e}"); sys.exit(1)

# ── Config ────────────────────────────────────────────────────────────────────
SYMBOLS    = ["NVDA","TSLA","AAPL","AMD","META","MSFT","GOOGL","AMZN",
              "MU","ORCL","NOW","AVGO","PLTR","DELL"]
REGIME_SYM = "SPY"
START      = date(2025, 4, 1)
END        = date(2026, 4, 8)
RISK_PCT   = 0.025
STOP_MULT  = 2.5
TP1_MULT   = 0.75
TP2_MULT   = 2.0
GRADE_MIN  = 60
TRADE_SIZE = 500.0   # $ per trade
TRAIL_2HR  = 0.40    # 40% of open→close range = proxy 2-hour price
DB_PATH    = os.path.join(os.path.dirname(os.path.dirname(
                 os.path.abspath(__file__))), "trader.db")
BATCH_ID   = f"12m_{datetime.now().strftime('%Y%m%d')}"

US_HOLIDAYS = {
    date(2025,1,1),  date(2025,1,20), date(2025,2,17), date(2025,4,18),
    date(2025,5,26), date(2025,6,19), date(2025,7,4),  date(2025,9,1),
    date(2025,11,27),date(2025,12,25),
    date(2026,1,1),  date(2026,1,19), date(2026,2,16), date(2026,4,3),
}

# ── Helpers ───────────────────────────────────────────────────────────────────
def trading_days(start, end):
    days, d = [], start
    while d <= end:
        if d.weekday() < 5 and d not in US_HOLIDAYS:
            days.append(d)
        d += timedelta(days=1)
    return days


def rsi_series(closes, p=14):
    s = pd.Series(closes)
    d = s.diff(1)
    g = d.clip(lower=0).rolling(p).mean()
    l = (-d.clip(upper=0)).rolling(p).mean()
    rs = g / l.replace(0, 1e-9)
    return 100 - 100 / (1 + rs)


def score_symbol(df):
    """Score 0-100 from RSI, volume ratio, momentum, gap."""
    if len(df) < 20:
        return 0, []
    cl = df["Close"].values
    vo = df["Volume"].values
    rsi_val = float(rsi_series(cl).iloc[-1]) if not pd.isna(rsi_series(cl).iloc[-1]) else 50
    vol_avg  = float(np.mean(vo[-20:]))
    vol_r    = float(vo[-1]) / max(vol_avg, 1)
    mom_5    = (cl[-1] - cl[-6]) / cl[-6] * 100 if len(cl) >= 6 else 0
    gap      = (cl[-1] - cl[-2]) / cl[-2] * 100 if len(cl) >= 2 else 0
    signals, score = [], 30
    if   40 <= rsi_val <= 65: score += 15; signals.append("RSI_OPTIMAL")
    elif rsi_val < 30:        score += 10; signals.append("RSI_OVERSOLD")
    elif rsi_val > 70:        score +=  5; signals.append("RSI_OVERBOUGHT")
    if   vol_r >= 2.0:        score += 20; signals.append("VOLUME_SURGE")
    elif vol_r >= 1.5:        score += 12; signals.append("VOLUME_ELEVATED")
    elif vol_r >= 1.2:        score +=  5
    if   mom_5 >= 3:          score += 15; signals.append("BULL_MOMENTUM")
    elif mom_5 >= 1:          score +=  8
    elif mom_5 <= -3:         score +=  5; signals.append("BEAR_MOMENTUM")
    if   abs(gap) >= 2:       score += 10; signals.append("GAP_UP" if gap > 0 else "GAP_DOWN")
    elif abs(gap) >= 1:       score +=  5
    return min(100, max(0, score)), signals


def compute_levels(df, price):
    if len(df) >= 14:
        hi = df["High"].values[-14:]
        lo = df["Low"].values[-14:]
        cl = df["Close"].values[-14:]
        tr = np.maximum(hi - lo,
             np.maximum(abs(hi - np.roll(cl, 1)), abs(lo - np.roll(cl, 1))))
        atr = float(np.mean(tr[1:]))
    else:
        atr = price * RISK_PCT / STOP_MULT
    risk       = atr * 1.5
    support    = round(float(df["Low"].values[-10:].min()),  2) if len(df) >= 10 else round(price * .97, 2)
    resistance = round(float(df["High"].values[-10:].max()), 2) if len(df) >= 10 else round(price * 1.03, 2)
    return {
        "atr":       round(atr, 2),
        "risk":      round(risk, 2),
        "stop":      round(price - risk * STOP_MULT, 2),
        "tp1":       round(price + risk * TP1_MULT, 2),
        "tp2":       round(price + risk * TP2_MULT, 2),
        "tp3":       resistance,
        "entry_lo":  round(price - atr * 0.3, 2),
        "entry_hi":  round(price + atr * 0.1, 2),
        "support":   support,
        "resistance":resistance,
    }


def detect_regime(spy_sub):
    if len(spy_sub) < 21:
        return "UNKNOWN"
    cl  = spy_sub["Close"].values
    p   = cl[-1]
    ma8  = np.mean(cl[-8:])
    ma21 = np.mean(cl[-21:])
    ma50 = np.mean(cl[-50:]) if len(cl) >= 50 else ma21
    if   p > ma8 > ma21:                    return "BULL_STRONG"
    elif p > ma21:                          return "BULL"
    elif p < ma8 < ma21 and p < ma50 * .95: return "CRISIS"
    elif p < ma8 < ma21:                    return "BEAR_STRONG"
    elif p < ma21:                          return "BEAR"
    return "NEUTRAL"


# ── Strategy simulators ───────────────────────────────────────────────────────

def _fill(o, h, l, lvl):
    """Returns fill_price or None if zone missed."""
    elo, ehi = lvl["entry_lo"], lvl["entry_hi"]
    if not (l <= ehi and h >= elo):
        return None
    return round(min(max(elo, o), ehi), 2)


def sim_with_timestop(o, h, l, c, lvl):
    """Strategy A: TP1=0.75R full exit if hit, else time-stop at ~11AM proxy."""
    fill = _fill(o, h, l, lvl)
    if fill is None:
        return None, "ZONE_MISS", 0.0
    stop, tp1 = lvl["stop"], lvl["tp1"]
    qty = TRADE_SIZE / fill
    # Gap through stop
    if fill <= stop:
        return fill, "STOP_GAP", round((stop - fill) / fill * TRADE_SIZE, 2)
    # Hard stop intraday
    if l <= stop:
        return fill, "STOP_LOSS", round((stop - fill) * qty, 2)
    # TP1 hit → full exit at TP1 (best-case captured before 11AM)
    if h >= tp1:
        return fill, "TP1_FULL",  round((tp1 - fill) * qty, 2)
    # Time stop: exit at ~2-hour price proxy (open + 40% drift toward close)
    price_2hr = round(o + (c - o) * TRAIL_2HR, 2)
    price_2hr = max(l, min(h, price_2hr))
    return fill, "TIME_STOP",  round((price_2hr - fill) * qty, 2)


def sim_no_timestop(o, h, l, c, lvl):
    """Strategy B: tiered TP1=0.75R (50%), TP2=2R (25%), TP3=wall (25%), EOD rest."""
    fill = _fill(o, h, l, lvl)
    if fill is None:
        return None, "ZONE_MISS", 0.0
    stop, tp1, tp2, tp3 = lvl["stop"], lvl["tp1"], lvl["tp2"], lvl["tp3"]
    qty = TRADE_SIZE / fill
    if fill <= stop:
        return fill, "STOP_GAP",  round((stop - fill) / fill * TRADE_SIZE, 2)
    if l <= stop:
        return fill, "STOP_LOSS", round((stop - fill) * qty, 2)
    pnl, rem = 0.0, qty
    if h >= tp1:
        t1q = rem * 0.5;  pnl += (tp1 - fill) * t1q; rem -= t1q
        if h >= tp2:
            t2q = qty * 0.25; pnl += (tp2 - fill) * t2q; rem -= t2q
            if h >= tp3:
                pnl += (tp3 - fill) * rem
                return fill, "TP3",      round(pnl, 2)
            pnl += (c - fill) * rem
            return fill, "TP2+EOD",  round(pnl, 2)
        pnl += (c - fill) * rem
        return fill, "TP1+EOD",  round(pnl, 2)
    pnl += (c - fill) * qty
    return fill, "EOD",        round(pnl, 2)


# ── DB ────────────────────────────────────────────────────────────────────────

def setup_db():
    conn = sqlite3.connect(DB_PATH)
    conn.execute("""
        CREATE TABLE IF NOT EXISTS ollie_backtest_30d (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            run_date TEXT, trade_date TEXT, symbol TEXT, grade TEXT,
            score INTEGER, regime TEXT, entry_lo REAL, entry_hi REAL,
            filled INTEGER, fill_price REAL, stop REAL, tp1 REAL, tp2 REAL,
            tp3 REAL, day_high REAL, day_low REAL, exit_price REAL,
            exit_reason TEXT, pnl REAL, signals TEXT
        )
    """)
    cols = [r[1] for r in conn.execute("PRAGMA table_info(ollie_backtest_30d)").fetchall()]
    for col, typedef in [("batch_id", "TEXT"), ("strategy", "TEXT DEFAULT 'A_baseline'")]:
        if col not in cols:
            conn.execute(f"ALTER TABLE ollie_backtest_30d ADD COLUMN {col} {typedef}")
            print(f"  [DB] Added {col} column")
    conn.commit()
    return conn


def db_insert(conn, row):
    conn.execute("""
        INSERT INTO ollie_backtest_30d
        (run_date,trade_date,symbol,grade,score,regime,entry_lo,entry_hi,
         filled,fill_price,stop,tp1,tp2,tp3,day_high,day_low,exit_price,
         exit_reason,pnl,signals,batch_id,strategy)
        VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
    """, (row["run_date"], row["trade_date"], row["symbol"], row["grade"],
          row["score"], row["regime"], row["entry_lo"], row["entry_hi"],
          row["filled"], row["fill_price"], row["stop"], row["tp1"],
          row["tp2"], row["tp3"], row["day_high"], row["day_low"],
          row["exit_price"], row["exit_reason"], row["pnl"],
          row["signals"], row["batch_id"], row["strategy"]))


# ── Stats ─────────────────────────────────────────────────────────────────────

def stats(trades):
    filled  = [t for t in trades if t["filled"]]
    wins    = [t for t in filled if t["pnl"] > 0]
    losses  = [t for t in filled if t["pnl"] <= 0]
    total   = sum(t["pnl"] for t in filled)
    wr      = len(wins) / max(len(filled), 1) * 100
    avg_w   = np.mean([t["pnl"] for t in wins])   if wins   else 0
    avg_l   = np.mean([t["pnl"] for t in losses]) if losses else 0
    # Max drawdown
    cum, peak, mdd = 0.0, 0.0, 0.0
    for t in filled:
        cum += t["pnl"]
        peak = max(peak, cum)
        mdd  = max(mdd, peak - cum)
    # Daily Sharpe
    dly = defaultdict(float)
    for t in filled:
        dly[t["trade_date"]] += t["pnl"]
    vals   = list(dly.values())
    sharpe = (np.mean(vals) / np.std(vals) * np.sqrt(252)) if len(vals) > 1 and np.std(vals) > 0 else 0
    # Exit reasons
    reasons = {}
    for t in filled:
        r = t["exit_reason"]; reasons[r] = reasons.get(r, 0) + 1
    # Best / worst day
    best_day  = max(dly.items(), key=lambda x: x[1])  if dly else ("—", 0)
    worst_day = min(dly.items(), key=lambda x: x[1])  if dly else ("—", 0)
    return dict(signals=len(trades), filled=len(filled),
                winners=len(wins), losers=len(losses),
                wr=wr, total=total, mdd=mdd, sharpe=sharpe,
                avg_win=avg_w, avg_loss=avg_l,
                best_day=best_day, worst_day=worst_day,
                reasons=reasons, daily=dly)


def print_stats(label, s):
    W = 62
    print("\n" + "=" * W)
    print(f"  {label}")
    print("=" * W)
    print(f"  {'Signals fired':<28} {s['signals']:>6}")
    print(f"  {'Filled trades':<28} {s['filled']:>6}")
    print(f"  {'Winners':<28} {s['winners']:>6}")
    print(f"  {'Losers':<28} {s['losers']:>6}")
    print(f"  {'Win rate':<28} {s['wr']:>5.1f}%")
    print(f"  {'Avg win':<28} ${s['avg_win']:>+7.2f}")
    print(f"  {'Avg loss':<28} ${s['avg_loss']:>+7.2f}")
    print(f"  {'Total P&L':<28} ${s['total']:>+8.2f}")
    print(f"  {'Max drawdown':<28} ${s['mdd']:>8.2f}")
    print(f"  {'Ann. Sharpe':<28} {s['sharpe']:>8.2f}")
    print(f"  {'Best day':<28} {s['best_day'][0]}  ${s['best_day'][1]:>+7.2f}")
    print(f"  {'Worst day':<28} {s['worst_day'][0]}  ${s['worst_day'][1]:>+7.2f}")
    print(f"\n  Exit reasons:")
    for r, n in sorted(s['reasons'].items(), key=lambda x: -x[1]):
        print(f"    {r:<22} {n:>4}  ({n/max(s['filled'],1)*100:.0f}%)")


# ── Main ──────────────────────────────────────────────────────────────────────

def main():
    print("=" * 72)
    print("OLLIE SUPER TRADER — 12-MONTH BACKTEST")
    print(f"Period  : {START} → {END}")
    print(f"Batch   : {BATCH_ID}")
    print(f"Symbols : {', '.join(SYMBOLS)}")
    print("=" * 72)

    # ── Confirm prior rows intact ──────────────────────────────────────────
    conn_check = sqlite3.connect(DB_PATH)
    rows_60d = conn_check.execute(
        "SELECT COUNT(*) FROM ollie_backtest_30d WHERE batch_id LIKE '60d_%'"
    ).fetchone()[0]
    rows_30d = conn_check.execute(
        "SELECT COUNT(*) FROM ollie_backtest_30d WHERE batch_id IS NULL"
    ).fetchone()[0]
    conn_check.close()
    print(f"\n  Prior rows intact — 60d batch: {rows_60d}  |  original 30d: {rows_30d}")
    assert rows_60d == 168, f"Expected 168 60d rows, got {rows_60d}"
    print("  ✓ 168 rows from 60-day run confirmed\n")

    # ── Download data ──────────────────────────────────────────────────────
    days    = trading_days(START, END)
    warmup  = START - timedelta(days=120)   # 4-month warmup for indicators
    dl_end  = END + timedelta(days=1)
    print(f"Trading days in range: {len(days)}")
    print(f"Downloading OHLCV {warmup} → {END} for {len(SYMBOLS)+1} symbols...")

    all_syms = SYMBOLS + [REGIME_SYM]
    raw = yf.download(
        all_syms,
        start=warmup.strftime("%Y-%m-%d"),
        end=dl_end.strftime("%Y-%m-%d"),
        auto_adjust=True, progress=False, group_by="ticker",
    )

    data = {}
    failed = []
    for sym in all_syms:
        try:
            if sym in raw.columns.get_level_values(0):
                df = raw[sym].dropna(subset=["Close"])
            else:
                df = raw.xs(sym, level=1, axis=1).dropna(subset=["Close"])
            if len(df) > 10:
                data[sym] = df
            else:
                raise ValueError("too few rows")
        except Exception:
            try:
                df = yf.download(sym, start=warmup.strftime("%Y-%m-%d"),
                                 end=dl_end.strftime("%Y-%m-%d"),
                                 auto_adjust=True, progress=False)
                df = df.dropna(subset=["Close"])
                if len(df) > 10:
                    data[sym] = df
                else:
                    failed.append(sym)
            except Exception:
                failed.append(sym)

    if failed:
        print(f"  WARNING: failed to load {failed} — skipping these symbols")
    syms_ok = [s for s in SYMBOLS if s in data]
    spy_df  = data.get(REGIME_SYM, pd.DataFrame())
    print(f"  Loaded {len(data)} symbols OK  (SPY regime available: {len(spy_df)>0})")

    # ── Run backtest ───────────────────────────────────────────────────────
    conn     = setup_db()
    run_date = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

    all_A, all_B = [], []  # collect across all days

    print(f"\n{'DATE':<12} {'REGIME':<12} {'SIG':>4} {'FILL':>4}  "
          f"{'PNL_A(stop)':>12}  {'PNL_B(notsp)':>12}  {'DIFF':>8}")
    print("-" * 72)

    for trade_date in days:
        ds = trade_date.strftime("%Y-%m-%d")

        spy_sub = spy_df[spy_df.index.date <= trade_date] if len(spy_df) > 0 else pd.DataFrame()
        regime  = detect_regime(spy_sub)
        skip    = regime in ("CRISIS", "BEAR_STRONG")

        day_A, day_B = [], []

        if not skip:
            for sym in syms_ok:
                df_sym = data[sym]
                prior  = df_sym[df_sym.index.date <  trade_date]
                today  = df_sym[df_sym.index.date == trade_date]
                if len(prior) < 15 or len(today) == 0:
                    continue

                o = float(today["Open"].iloc[0])
                h = float(today["High"].iloc[0])
                l = float(today["Low"].iloc[0])
                c = float(today["Close"].iloc[0])
                price = float(prior["Close"].iloc[-1])

                score, signals = score_symbol(prior)
                if score < GRADE_MIN:
                    continue
                grade = "A" if score>=80 else "B" if score>=65 else "C" if score>=50 else "D"
                # Only A/B trades in BEAR regime
                if regime == "BEAR" and grade not in ("A", "B"):
                    continue

                lvl = compute_levels(prior, price)

                base = dict(
                    run_date=run_date, trade_date=ds, symbol=sym,
                    grade=grade, score=score, regime=regime,
                    entry_lo=lvl["entry_lo"], entry_hi=lvl["entry_hi"],
                    stop=lvl["stop"], tp1=lvl["tp1"], tp2=lvl["tp2"],
                    tp3=lvl["tp3"], day_high=h, day_low=l,
                    signals=json.dumps(signals), batch_id=BATCH_ID,
                )

                fill_a, reas_a, pnl_a = sim_with_timestop(o, h, l, c, lvl)
                fill_b, reas_b, pnl_b = sim_no_timestop(o, h, l, c, lvl)

                def exit_px(fill, pnl, qty_base=TRADE_SIZE):
                    if not fill: return None
                    qty = qty_base / fill
                    return round(fill + pnl / qty, 2) if qty > 0 else None

                row_a = {**base, "strategy": "A_timestop",
                         "filled": 1 if fill_a else 0, "fill_price": fill_a,
                         "exit_price": exit_px(fill_a, pnl_a), "exit_reason": reas_a, "pnl": pnl_a}
                row_b = {**base, "strategy": "B_no_timestop",
                         "filled": 1 if fill_b else 0, "fill_price": fill_b,
                         "exit_price": exit_px(fill_b, pnl_b), "exit_reason": reas_b, "pnl": pnl_b}

                db_insert(conn, row_a)
                db_insert(conn, row_b)
                day_A.append(row_a)
                day_B.append(row_b)

        conn.commit()
        all_A.extend(day_A)
        all_B.extend(day_B)

        if day_A or skip:
            pnl_a_d = sum(r["pnl"] for r in day_A)
            pnl_b_d = sum(r["pnl"] for r in day_B)
            fills   = sum(1 for r in day_A if r["filled"])
            note    = " (SKIP)" if skip else ""
            diff    = pnl_a_d - pnl_b_d
            print(f"{ds:<12} {regime:<12} {len(day_A):>4} {fills:>4}  "
                  f"{pnl_a_d:>+12.2f}  {pnl_b_d:>+12.2f}  {diff:>+8.2f}{note}")

    conn.close()

    # ── Stats ──────────────────────────────────────────────────────────────
    sA = stats(all_A)
    sB = stats(all_B)

    # ── Side-by-side comparison ────────────────────────────────────────────
    W = 72
    print("\n\n" + "=" * W)
    print("12-MONTH STRATEGY COMPARISON")
    print("=" * W)
    rows_fmt = [
        ("Signals fired",  "signals",   "{:>6}"),
        ("Filled trades",  "filled",    "{:>6}"),
        ("Winners",        "winners",   "{:>6}"),
        ("Losers",         "losers",    "{:>6}"),
        ("Win rate",       "wr",        "{:>5.1f}%"),
        ("Avg win",        "avg_win",   "${:>+7.2f}"),
        ("Avg loss",       "avg_loss",  "${:>+7.2f}"),
        ("Total P&L",      "total",     "${:>+8.2f}"),
        ("Max drawdown",   "mdd",       "${:>8.2f}"),
        ("Ann. Sharpe",    "sharpe",    "{:>8.2f}"),
    ]
    print(f"  {'Metric':<28}  {'A: With Time Stop':>18}  {'B: No Time Stop':>18}  {'Delta':>9}")
    print("  " + "-" * 68)
    for label, key, fmt in rows_fmt:
        va, vb = sA[key], sB[key]
        delta  = va - vb
        def f(v): return fmt.format(v)
        delta_s = f"+{delta:.2f}" if isinstance(delta, float) else str(delta)
        best = "◀" if va > vb else ("▶" if vb > va else " ")
        print(f"  {label:<28}  {f(va):>18}  {f(vb):>18}  {delta_s:>9}  {best}")
    print()

    print_stats("Strategy A — WITH Time Stop  (11AM ET exit if no TP1)", sA)
    print_stats("Strategy B — WITHOUT Time Stop (tiered TPs, EOD fallback)", sB)

    # ── Monthly P&L breakdown ──────────────────────────────────────────────
    print("\n\n" + "=" * W)
    print("MONTHLY P&L BREAKDOWN")
    print("=" * W)
    print(f"  {'Month':<10}  {'TradeDays':>9}  {'Fills_A':>7}  {'PNL_A':>9}  {'Fills_B':>7}  {'PNL_B':>9}  {'Regime'}")
    print("  " + "-" * 68)
    monthly_a = defaultdict(float)
    monthly_b = defaultdict(float)
    monthly_fills_a = defaultdict(int)
    monthly_fills_b = defaultdict(int)
    for t in all_A:
        ym = t["trade_date"][:7]
        monthly_a[ym] += t["pnl"]
        if t["filled"]: monthly_fills_a[ym] += 1
    for t in all_B:
        ym = t["trade_date"][:7]
        monthly_b[ym] += t["pnl"]
        if t["filled"]: monthly_fills_b[ym] += 1
    all_months = sorted(set(list(monthly_a.keys()) + list(monthly_b.keys())))
    for ym in all_months:
        pa = monthly_a.get(ym, 0); pb = monthly_b.get(ym, 0)
        fa = monthly_fills_a.get(ym, 0); fb = monthly_fills_b.get(ym, 0)
        days_in_month = sum(1 for d in days if d.strftime("%Y-%m") == ym)
        print(f"  {ym:<10}  {days_in_month:>9}  {fa:>7}  {pa:>+9.2f}  {fb:>7}  {pb:>+9.2f}")

    # ── Daily P&L (active days only) ──────────────────────────────────────
    print("\n\n" + "=" * W)
    print("DAILY P&L — Strategy A (With Time Stop), active days only")
    print("=" * W)
    daily_a = sA["daily"]
    cum_a   = 0.0
    print(f"  {'Date':<12}  {'P&L':>9}  {'Cumulative':>11}")
    print("  " + "-" * 36)
    for ds in sorted(daily_a.keys()):
        cum_a += daily_a[ds]
        arrow = "↑" if daily_a[ds] > 0 else "↓"
        print(f"  {ds:<12}  {daily_a[ds]:>+9.2f}  {cum_a:>+11.2f}  {arrow}")

    # ── Full trade-by-trade table (Strategy A, filled only) ───────────────
    filled_A = [t for t in all_A if t["filled"]]
    print("\n\n" + "=" * W)
    print(f"STRATEGY A — FULL TRADE TABLE  ({len(filled_A)} filled trades)")
    print("=" * W)
    H = ("{:<12} {:<6} {:<3} {:<4} {:<11} {:>8} {:>8} {:>8} {:>9} {:>9} {:>9} {:<13} {:>8}")
    print(H.format("DATE","SYM","GR","SCR","REGIME","FILL","STOP","TP1","HIGH","LOW","EXIT","REASON","PNL"))
    print("-" * 118)
    for t in filled_A:
        fp = f"${t['fill_price']:.2f}" if t["fill_price"] else "--"
        ep = f"${t['exit_price']:.2f}" if t["exit_price"] else "--"
        print(H.format(
            t["trade_date"], t["symbol"], t["grade"], t["score"], t["regime"],
            fp,
            f"${t['stop']:.2f}",
            f"${t['tp1']:.2f}",
            f"${t['day_high']:.2f}",
            f"${t['day_low']:.2f}",
            ep,
            t["exit_reason"],
            f"${t['pnl']:+.2f}",
        ))

    # ── Summary footer ─────────────────────────────────────────────────────
    print(f"\n\nRows saved → trader.db :: ollie_backtest_30d  batch={BATCH_ID}")
    total_rows = len(all_A) + len(all_B)
    print(f"  This run: {total_rows} rows  ({len(all_A)} Strategy A  +  {len(all_B)} Strategy B)")
    conn2 = sqlite3.connect(DB_PATH)
    grand_total = conn2.execute("SELECT COUNT(*) FROM ollie_backtest_30d").fetchone()[0]
    conn2.close()
    print(f"  Grand total rows in table: {grand_total}")


if __name__ == "__main__":
    main()
