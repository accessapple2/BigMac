#!/usr/bin/env python3
"""
Ollie Super Trader — 60-Day Backtest + Strategy Comparison
Feb 1 – April 8, 2026

Three strategies tested side-by-side on the same fills:
  A) Baseline   — TP1=0.75R, TP2=2R, TP3=resistance
  B) Trail Stop — after TP1 hit, trail 1.5% below day high
  C) Time Stop  — exit at EOD if TP1 not hit within first 2hr (approx: price < tp1 by 11AM open + 2hr)

Adds batch_id column to ollie_backtest_30d (ALTER IF NOT EXISTS).
INSERTs new rows — never deletes old ones.

Usage:
  venv/bin/python3 scripts/ollie_backtest_60d.py
"""
import sqlite3, json, sys, os, textwrap
from datetime import datetime, timedelta, date

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

try:
    import yfinance as yf
    import pandas as pd
    import numpy as np
except ImportError as e:
    print(f"Missing dep: {e}"); sys.exit(1)

# ── Config ────────────────────────────────────────────────────────────────────
SYMBOLS = ["NVDA","TSLA","AAPL","AMD","META","MSFT","GOOGL","AMZN",
           "MU","ORCL","NOW","AVGO","PLTR","DELL"]
REGIME_SYM  = "SPY"
START       = date(2026, 2, 1)
END         = date(2026, 4, 8)
RISK_PCT    = 0.025
STOP_MULT   = 2.5
TP1_MULT    = 0.75   # 0.75× risk
TP2_MULT    = 2.0
GRADE_B_MIN = 60
TRADE_SIZE  = 500.0   # $ per trade
TRAIL_PCT   = 0.015   # 1.5% trail after TP1
DB_PATH     = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "trader.db")
BATCH_ID    = f"60d_{datetime.now().strftime('%Y%m%d_%H%M')}"

US_HOLIDAYS_2026 = {
    date(2026,1,1), date(2026,1,19), date(2026,2,16),
    date(2026,4,3), date(2026,5,25), date(2026,7,3),
    date(2026,9,7), date(2026,11,26), date(2026,12,25),
}

# ── Helpers ───────────────────────────────────────────────────────────────────
def trading_days(start, end):
    days, d = [], start
    while d <= end:
        if d.weekday() < 5 and d not in US_HOLIDAYS_2026:
            days.append(d)
        d += timedelta(days=1)
    return days


def rsi_series(closes, p=14):
    s = pd.Series(closes)
    d = s.diff(1)
    g = d.clip(lower=0).rolling(p).mean()
    l = (-d.clip(upper=0)).rolling(p).mean()
    rs = g / l.replace(0, 1e-9)
    return 100 - 100/(1+rs)


def score_symbol(df):
    if len(df) < 20: return 0, []
    closes  = df["Close"].values
    volumes = df["Volume"].values
    rsi_val = float(rsi_series(closes).iloc[-1]) if not pd.isna(rsi_series(closes).iloc[-1]) else 50
    vol_avg = float(np.mean(volumes[-20:]))
    vol_ratio = float(volumes[-1]) / max(vol_avg, 1)
    mom_5d = (closes[-1]-closes[-6])/closes[-6]*100 if len(closes)>=6 else 0
    gap    = (closes[-1]-closes[-2])/closes[-2]*100 if len(closes)>=2 else 0
    signals, score = [], 30
    if 40<=rsi_val<=65:   score+=15; signals.append("RSI_OPTIMAL")
    elif rsi_val<30:      score+=10; signals.append("RSI_OVERSOLD")
    elif rsi_val>70:      score+=5;  signals.append("RSI_OVERBOUGHT")
    if   vol_ratio>=2.0:  score+=20; signals.append("VOLUME_SURGE")
    elif vol_ratio>=1.5:  score+=12; signals.append("VOLUME_ELEVATED")
    elif vol_ratio>=1.2:  score+=5
    if   mom_5d>=3:       score+=15; signals.append("BULL_MOMENTUM")
    elif mom_5d>=1:       score+=8
    elif mom_5d<=-3:      score+=5;  signals.append("BEAR_MOMENTUM")
    if   abs(gap)>=2:     score+=10; signals.append("GAP_UP" if gap>0 else "GAP_DOWN")
    elif abs(gap)>=1:     score+=5
    return min(100, max(0, score)), signals


def compute_levels(df, price):
    if len(df)<14:
        atr = price * RISK_PCT / STOP_MULT
    else:
        hi = df["High"].values[-14:]
        lo = df["Low"].values[-14:]
        cl = df["Close"].values[-14:]
        tr = np.maximum(hi-lo, np.maximum(abs(hi-np.roll(cl,1)), abs(lo-np.roll(cl,1))))
        atr = float(np.mean(tr[1:]))
    risk       = atr * 1.5
    support    = round(float(df["Low"].values[-10:].min()), 2)  if len(df)>=10 else round(price*.97,2)
    resistance = round(float(df["High"].values[-10:].max()), 2) if len(df)>=10 else round(price*1.03,2)
    return {
        "atr":      round(atr,2),
        "risk":     round(risk,2),
        "stop":     round(price - risk*STOP_MULT,2),
        "tp1":      round(price + risk*TP1_MULT,2),
        "tp2":      round(price + risk*TP2_MULT,2),
        "tp3":      resistance,
        "entry_lo": round(price - atr*0.3,2),
        "entry_hi": round(price + atr*0.1,2),
        "support":  support,
        "resistance":resistance,
    }


def detect_regime(spy_sub):
    if len(spy_sub)<21: return "UNKNOWN"
    cl = spy_sub["Close"].values
    p, ma8, ma21 = cl[-1], np.mean(cl[-8:]), np.mean(cl[-21:])
    ma50 = np.mean(cl[-50:]) if len(cl)>=50 else ma21
    if p>ma8>ma21:            return "BULL_STRONG"
    elif p>ma21:              return "BULL"
    elif p<ma8<ma21 and p<ma50*.95: return "CRISIS"
    elif p<ma8<ma21:          return "BEAR_STRONG"
    elif p<ma21:              return "BEAR"
    return "NEUTRAL"


# ── Three strategy simulators ─────────────────────────────────────────────────

def sim_baseline(o, h, l, c, lvl):
    """Strategy A: TP1=0.75R, TP2=2R, TP3=resistance, tiered exits."""
    elo, ehi = lvl["entry_lo"], lvl["entry_hi"]
    stop, tp1, tp2, tp3 = lvl["stop"], lvl["tp1"], lvl["tp2"], lvl["tp3"]
    # Fill check
    if not (l<=ehi and h>=elo):
        return None, "ZONE_MISS", 0.0
    fill = round(min(max(elo, o), ehi), 2)
    if fill<=stop:
        return fill, "STOP_GAP", round((stop-fill)/fill*TRADE_SIZE, 2)
    qty = TRADE_SIZE / fill
    if l<=stop:
        return fill, "STOP_LOSS", round((stop-fill)*qty, 2)
    pnl, rem = 0.0, qty
    if h>=tp1:
        t1q = rem*0.5; pnl += (tp1-fill)*t1q; rem -= t1q
        if h>=tp2:
            t2q = qty*0.25; pnl += (tp2-fill)*t2q; rem -= t2q
            if h>=tp3:
                pnl += (tp3-fill)*rem
                return fill, "TP3",     round(pnl,2)
            pnl += (c-fill)*rem
            return fill, "TP2+EOD", round(pnl,2)
        pnl += (c-fill)*rem
        return fill, "TP1+EOD", round(pnl,2)
    pnl += (c-fill)*rem
    return fill, "EOD", round(pnl,2)


def sim_trail(o, h, l, c, lvl):
    """Strategy B: same entry/TP1, but after TP1 trail 1.5% below day high."""
    elo, ehi = lvl["entry_lo"], lvl["entry_hi"]
    stop, tp1 = lvl["stop"], lvl["tp1"]
    if not (l<=ehi and h>=elo):
        return None, "ZONE_MISS", 0.0
    fill = round(min(max(elo, o), ehi), 2)
    if fill<=stop:
        return fill, "STOP_GAP", round((stop-fill)/fill*TRADE_SIZE, 2)
    qty = TRADE_SIZE / fill
    if l<=stop:
        return fill, "STOP_LOSS", round((stop-fill)*qty, 2)
    if h>=tp1:
        # TP1 triggers: exit 50% at tp1, trail rest
        t1q  = qty*0.5
        pnl  = (tp1-fill)*t1q
        rem  = qty - t1q
        # Trail stop = day_high * (1 - TRAIL_PCT)
        # Worst case: high after TP1 = h, trail fires at h*(1-TRAIL_PCT)
        trail_exit = round(h*(1-TRAIL_PCT), 2)
        # If trail is still above fill, take it; else EOD
        if trail_exit > fill:
            pnl += (trail_exit-fill)*rem
            return fill, "TP1+TRAIL", round(pnl,2)
        pnl += (c-fill)*rem
        return fill, "TP1+EOD_T", round(pnl,2)
    pnl = (c-fill)*qty
    return fill, "EOD", round(pnl,2)


def sim_timestop(o, h, l, c, lvl):
    """Strategy C: exit at EOD if TP1 not reached. Simulated 'time stop' using
    a proxy: if TP1 is hit (any time during day), take it fully. Otherwise, exit
    at close of first 2 hours (approx. 11AM price = open * slight drift).
    We model 2-hr price as midpoint of open and close, capped at day range."""
    elo, ehi = lvl["entry_lo"], lvl["entry_hi"]
    stop, tp1 = lvl["stop"], lvl["tp1"]
    if not (l<=ehi and h>=elo):
        return None, "ZONE_MISS", 0.0
    fill = round(min(max(elo, o), ehi), 2)
    if fill<=stop:
        return fill, "STOP_GAP", round((stop-fill)/fill*TRADE_SIZE, 2)
    qty = TRADE_SIZE / fill
    if l<=stop:
        return fill, "STOP_LOSS", round((stop-fill)*qty, 2)
    if h>=tp1:
        # TP1 hit within session — take full position at TP1
        pnl = (tp1-fill)*qty
        return fill, "TP1_FULL",  round(pnl,2)
    # TP1 not hit — simulate time stop at ~2hr price (open + 40% drift toward close)
    price_2hr = round(o + (c-o)*0.4, 2)
    price_2hr = max(l, min(h, price_2hr))  # clamp to day range
    pnl = (price_2hr-fill)*qty
    return fill, "TIME_STOP", round(pnl,2)


# ── DB setup ─────────────────────────────────────────────────────────────────

def setup_db():
    conn = sqlite3.connect(DB_PATH)
    # Create table if not exists (matches original schema)
    conn.execute("""
        CREATE TABLE IF NOT EXISTS ollie_backtest_30d (
            id          INTEGER PRIMARY KEY AUTOINCREMENT,
            run_date    TEXT, trade_date TEXT, symbol TEXT, grade TEXT,
            score INTEGER, regime TEXT, entry_lo REAL, entry_hi REAL,
            filled INTEGER, fill_price REAL, stop REAL, tp1 REAL, tp2 REAL, tp3 REAL,
            day_high REAL, day_low REAL, exit_price REAL, exit_reason TEXT,
            pnl REAL, signals TEXT
        )
    """)
    # Add batch_id column if missing
    cols = [r[1] for r in conn.execute("PRAGMA table_info(ollie_backtest_30d)").fetchall()]
    if "batch_id" not in cols:
        conn.execute("ALTER TABLE ollie_backtest_30d ADD COLUMN batch_id TEXT")
        print("  [DB] Added batch_id column to ollie_backtest_30d")
    if "strategy" not in cols:
        conn.execute("ALTER TABLE ollie_backtest_30d ADD COLUMN strategy TEXT DEFAULT 'A_baseline'")
        print("  [DB] Added strategy column to ollie_backtest_30d")
    conn.commit()
    return conn


def insert_trade(conn, row: dict):
    conn.execute("""
        INSERT INTO ollie_backtest_30d
        (run_date,trade_date,symbol,grade,score,regime,entry_lo,entry_hi,
         filled,fill_price,stop,tp1,tp2,tp3,day_high,day_low,exit_price,
         exit_reason,pnl,signals,batch_id,strategy)
        VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
    """, (
        row["run_date"], row["trade_date"], row["symbol"], row["grade"],
        row["score"], row["regime"], row["entry_lo"], row["entry_hi"],
        row["filled"], row["fill_price"], row["stop"], row["tp1"], row["tp2"],
        row["tp3"], row["day_high"], row["day_low"], row["exit_price"],
        row["exit_reason"], row["pnl"], row["signals"], row["batch_id"],
        row["strategy"],
    ))


# ── Stats helper ──────────────────────────────────────────────────────────────

def summarise(label, trades):
    filled = [t for t in trades if t["filled"]]
    wins   = [t for t in filled if t["pnl"]>0]
    losses = [t for t in filled if t["pnl"]<=0]
    total  = sum(t["pnl"] for t in filled)
    wr     = len(wins)/max(len(filled),1)*100
    # Max drawdown
    cum, peak, mdd = 0, 0, 0
    for t in filled:
        cum += t["pnl"]
        if cum>peak: peak=cum
        dd = peak-cum
        if dd>mdd: mdd=dd
    # Sharpe (daily)
    from collections import defaultdict
    dly = defaultdict(float)
    for t in filled: dly[t["trade_date"]] += t["pnl"]
    vals = list(dly.values())
    sharpe = (np.mean(vals)/np.std(vals)*np.sqrt(252)) if len(vals)>1 and np.std(vals)>0 else 0
    # Exit breakdown
    reasons = {}
    for t in filled:
        r = t["exit_reason"]; reasons[r] = reasons.get(r,0)+1
    return {
        "label":   label,
        "signals": len(trades),
        "filled":  len(filled),
        "winners": len(wins),
        "losers":  len(losses),
        "wr":      wr,
        "total":   total,
        "mdd":     mdd,
        "sharpe":  sharpe,
        "reasons": reasons,
    }


def print_summary(s):
    print(f"\n{'='*60}")
    print(f"  {s['label']}")
    print(f"{'='*60}")
    print(f"  Signals fired : {s['signals']}")
    print(f"  Filled trades : {s['filled']}")
    print(f"  Winners       : {s['winners']}")
    print(f"  Losers        : {s['losers']}")
    print(f"  Win rate      : {s['wr']:.1f}%")
    print(f"  Total P&L     : ${s['total']:+.2f}")
    print(f"  Max drawdown  : ${s['mdd']:.2f}")
    print(f"  Ann. Sharpe   : {s['sharpe']:.2f}")
    print(f"  Exit reasons  :")
    for r,n in sorted(s['reasons'].items(), key=lambda x:-x[1]):
        print(f"    {r:<20} {n:>4}")


# ── Main ──────────────────────────────────────────────────────────────────────

def main():
    print("="*72)
    print("OLLIE SUPER TRADER — 60-DAY BACKTEST + STRATEGY COMPARISON")
    print(f"Period : {START} → {END}")
    print(f"Batch  : {BATCH_ID}")
    print("="*72)

    days = trading_days(START, END)
    print(f"Trading days: {len(days)}")

    warmup = START - timedelta(days=90)
    print(f"Downloading OHLCV {warmup} → {END} ...")
    all_syms = SYMBOLS + [REGIME_SYM]
    try:
        raw = yf.download(all_syms,
                          start=warmup.strftime("%Y-%m-%d"),
                          end=(END+timedelta(days=1)).strftime("%Y-%m-%d"),
                          auto_adjust=True, progress=False, group_by="ticker")
    except Exception as e:
        print(f"Download error: {e}"); sys.exit(1)

    data = {}
    for sym in all_syms:
        try:
            if sym in raw.columns.get_level_values(0):
                df = raw[sym].dropna(subset=["Close"])
            else:
                df = raw.xs(sym, level=1, axis=1).dropna(subset=["Close"])
            data[sym] = df
        except Exception:
            try:
                df = yf.download(sym, start=warmup.strftime("%Y-%m-%d"),
                                 end=(END+timedelta(days=1)).strftime("%Y-%m-%d"),
                                 auto_adjust=True, progress=False)
                data[sym] = df.dropna(subset=["Close"])
            except Exception as ex:
                print(f"  WARNING: {sym} failed: {ex}"); data[sym] = pd.DataFrame()

    spy_df = data.get(REGIME_SYM, pd.DataFrame())
    conn   = setup_db()
    run_date = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

    # Collect all candidate trades first (same fills for all 3 strategies)
    candidates = []   # list of dicts with all day data

    print(f"\n{'DATE':<12} {'REGIME':<12} {'SIG':>4} {'FILL':>4}  {'PNL_A':>8}  {'PNL_B':>8}  {'PNL_C':>8}")
    print("-"*72)

    daily_rows = {}   # date_str → {"a":[], "b":[], "c":[]}

    for trade_date in days:
        ds = trade_date.strftime("%Y-%m-%d")
        spy_sub = spy_df[spy_df.index.date <= trade_date] if len(spy_df)>0 else pd.DataFrame()
        regime  = detect_regime(spy_sub)
        skip    = regime in ("CRISIS", "BEAR_STRONG")

        day_a, day_b, day_c = [], [], []

        for sym in SYMBOLS:
            df_sym = data.get(sym, pd.DataFrame())
            if len(df_sym)<20: continue
            prior  = df_sym[df_sym.index.date < trade_date]
            today  = df_sym[df_sym.index.date == trade_date]
            if len(prior)<15 or len(today)==0: continue

            o = float(today["Open"].iloc[0])
            h = float(today["High"].iloc[0])
            l = float(today["Low"].iloc[0])
            c = float(today["Close"].iloc[0])
            price = float(prior["Close"].iloc[-1])

            score, signals = score_symbol(prior)
            grade = "A" if score>=80 else "B" if score>=65 else "C" if score>=50 else "D" if score>=35 else "E"
            if score < GRADE_B_MIN: continue
            if skip: continue
            if regime=="BEAR" and grade not in ("A",): continue

            lvl = compute_levels(prior, price)

            base = {
                "run_date":  run_date, "trade_date": ds, "symbol": sym,
                "grade":     grade, "score": score, "regime": regime,
                "entry_lo":  lvl["entry_lo"], "entry_hi": lvl["entry_hi"],
                "stop": lvl["stop"], "tp1": lvl["tp1"], "tp2": lvl["tp2"],
                "tp3": lvl["tp3"], "day_high": h, "day_low": l,
                "signals":   json.dumps(signals), "batch_id": BATCH_ID,
            }

            # ── Strategy A ──
            fill_a, reason_a, pnl_a = sim_baseline(o, h, l, c, lvl)
            row_a = {**base, "strategy":"A_baseline",
                     "filled":1 if fill_a else 0, "fill_price":fill_a,
                     "exit_price": round(fill_a+(pnl_a/(TRADE_SIZE/fill_a) if fill_a else 0),2) if fill_a else None,
                     "exit_reason":reason_a, "pnl":pnl_a}
            day_a.append(row_a)

            # ── Strategy B ──
            fill_b, reason_b, pnl_b = sim_trail(o, h, l, c, lvl)
            row_b = {**base, "strategy":"B_trail",
                     "filled":1 if fill_b else 0, "fill_price":fill_b,
                     "exit_price": round(fill_b+(pnl_b/(TRADE_SIZE/fill_b) if fill_b else 0),2) if fill_b else None,
                     "exit_reason":reason_b, "pnl":pnl_b}
            day_b.append(row_b)

            # ── Strategy C ──
            fill_c, reason_c, pnl_c = sim_timestop(o, h, l, c, lvl)
            row_c = {**base, "strategy":"C_timestop",
                     "filled":1 if fill_c else 0, "fill_price":fill_c,
                     "exit_price": round(fill_c+(pnl_c/(TRADE_SIZE/fill_c) if fill_c else 0),2) if fill_c else None,
                     "exit_reason":reason_c, "pnl":pnl_c}
            day_c.append(row_c)

        for r in day_a + day_b + day_c:
            insert_trade(conn, r)
        conn.commit()

        pnl_a_day = sum(r["pnl"] for r in day_a)
        pnl_b_day = sum(r["pnl"] for r in day_b)
        pnl_c_day = sum(r["pnl"] for r in day_c)
        fills_a   = sum(1 for r in day_a if r["filled"])
        note = "(SKIP)" if skip else ""
        print(f"{ds:<12} {regime:<12} {len(day_a):>4} {fills_a:>4}  "
              f"{pnl_a_day:>+8.2f}  {pnl_b_day:>+8.2f}  {pnl_c_day:>+8.2f}  {note}")

        daily_rows[ds] = {"a": day_a, "b": day_b, "c": day_c}

    conn.close()

    # ── Flatten all trades per strategy ──────────────────────────────────────
    all_a = [r for v in daily_rows.values() for r in v["a"]]
    all_b = [r for v in daily_rows.values() for r in v["b"]]
    all_c = [r for v in daily_rows.values() for r in v["c"]]

    sa = summarise("Strategy A — Baseline (TP1=0.75R, TP2=2R, TP3=wall)", all_a)
    sb = summarise("Strategy B — Trail Stop (TP1+1.5% trail)", all_b)
    sc = summarise("Strategy C — Time Stop (exit 2hr if no TP1)", all_c)

    # ── Side-by-side comparison ───────────────────────────────────────────────
    print("\n\n" + "="*72)
    print("STRATEGY COMPARISON SUMMARY")
    print("="*72)
    metrics = [
        ("Signals fired",  "signals",  False, "{:>6}"),
        ("Filled trades",  "filled",   False, "{:>6}"),
        ("Win rate",       "wr",       False, "{:>5.1f}%"),
        ("Total P&L",      "total",    True,  "${:>+8.2f}"),
        ("Max drawdown",   "mdd",      False, "${:>8.2f}"),
        ("Sharpe (ann.)",  "sharpe",   False, "{:>8.2f}"),
    ]
    hdr = f"  {'Metric':<22}  {'A: Baseline':>14}  {'B: Trail Stop':>14}  {'C: Time Stop':>14}"
    print(hdr)
    print("  " + "-"*66)
    for label, key, is_money, fmt in metrics:
        va = sa[key]; vb = sb[key]; vc = sc[key]
        best_val = max(va, vb, vc) if not is_money else max(va, vb, vc)
        def f(v): return fmt.format(v)
        def mark(v): return f(v) + ("  ◀ BEST" if v==best_val and va!=vb or v==best_val and va!=vc else "")
        print(f"  {label:<22}  {f(va):>14}  {f(vb):>14}  {f(vc):>14}")
    print()

    print_summary(sa)
    print_summary(sb)
    print_summary(sc)

    # ── Full trade-by-trade table (Strategy A) ────────────────────────────────
    print("\n\n" + "="*72)
    print("STRATEGY A — FULL TRADE TABLE (filled trades)")
    print("="*72)
    filled_a = [t for t in all_a if t["filled"]]
    hdr2 = f"{'DATE':<12} {'SYM':<6} {'GR':<3} {'REGIME':<12} {'FILL':>8} {'STOP':>8} {'TP1':>8} {'TP2':>8} {'HIGH':>8} {'LOW':>8} {'EXIT':>8} {'REASON':<14} {'PNL':>8}"
    print(hdr2)
    print("-"*136)
    for t in filled_a:
        fp  = f"${t['fill_price']:.2f}"  if t['fill_price']  else "--"
        ep  = f"${t['exit_price']:.2f}"  if t['exit_price']  else "--"
        print(f"{t['trade_date']:<12} {t['symbol']:<6} {t['grade']:<3} {t['regime']:<12} "
              f"{fp:>8} ${t['stop']:.2f:>7} ${t['tp1']:.2f:>7} ${t['tp2']:.2f:>7} "
              f"${t['day_high']:.2f:>7} ${t['day_low']:.2f:>7} {ep:>8} {t['exit_reason']:<14} ${t['pnl']:>+7.2f}")

    print(f"\nAll {len(all_a)+len(all_b)+len(all_c)} rows saved → trader.db :: ollie_backtest_30d (batch_id={BATCH_ID})")


if __name__ == "__main__":
    main()
