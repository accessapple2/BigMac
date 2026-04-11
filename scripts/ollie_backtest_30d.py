#!/usr/bin/env python3
"""
Ollie Super Trader — 30-Day Backtest
March 1 – April 8, 2026

Methodology:
- Universe: WATCH_STOCKS from config (17 symbols, excl. SPY/QQQ/TQQQ used as regime indicators)
- Each trading day: score each symbol using technicals (RSI, volume, momentum)
- Grade B+ gate (score × 20 ≥ 60 → scaled_score ≥ 60)
- Regime gate: skip CRISIS/BEAR days (simulate using SPY vs 8/21 MA)
- Entry: must touch entry zone (lo = support, hi = price - 0.5×ATR)
- TP1 at 0.75×risk (50% size), TP2 at 2.0×risk (25%), TP3 at resistance (25%)
- Stop at 2.5×risk below entry
- EOD exit at close if no TP/stop hit
- Results saved to trader.db → ollie_backtest_30d

Usage:
  venv/bin/python3 scripts/ollie_backtest_30d.py
"""
import sqlite3
import json
import sys
import os
from datetime import datetime, timedelta, date

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

try:
    import yfinance as yf
    import pandas as pd
    import numpy as np
except ImportError as e:
    print(f"Missing dependency: {e}")
    sys.exit(1)

# ── Config ──────────────────────────────────────────────────────────────────
SYMBOLS = ["NVDA", "TSLA", "AAPL", "AMD", "META", "MSFT", "GOOGL", "AMZN",
           "MU", "ORCL", "NOW", "AVGO", "PLTR", "DELL"]
REGIME_SYMBOL = "SPY"
START = date(2026, 3, 1)
END   = date(2026, 4, 8)
RISK_PCT    = 0.025   # 2.5% of price
STOP_MULT   = 2.5     # stop = price - risk * STOP_MULT
TP1_MULT    = 0.75    # tp1  = price + risk * TP1_MULT
TP2_MULT    = 2.0
GRADE_B_MIN = 60      # scaled score threshold
TRADE_SIZE  = 500.0   # $ per trade (paper)
DB_PATH = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "trader.db")

# ── Helpers ──────────────────────────────────────────────────────────────────
def rsi(closes, period=14):
    diff = pd.Series(closes).diff(1)
    gain = diff.clip(lower=0)
    loss = -diff.clip(upper=0)
    avg_gain = gain.rolling(period).mean()
    avg_loss = loss.rolling(period).mean()
    rs = avg_gain / avg_loss.replace(0, 1e-9)
    return 100 - 100 / (1 + rs)


def score_symbol(df_sym):
    """
    Compute a 0-100 score using RSI, volume ratio, and momentum.
    Matches the Signal Center SCREENER logic closely.
    """
    if len(df_sym) < 20:
        return 0, []

    closes = df_sym["Close"].values
    volumes = df_sym["Volume"].values

    # RSI
    rsi_series = rsi(closes)
    rsi_val = float(rsi_series.iloc[-1]) if not pd.isna(rsi_series.iloc[-1]) else 50

    # Volume ratio (today vs 20-day avg)
    vol_avg = float(np.mean(volumes[-20:])) if len(volumes) >= 20 else float(volumes[-1])
    vol_ratio = float(volumes[-1]) / max(vol_avg, 1)

    # Momentum: 5-day return
    mom_5d = (closes[-1] - closes[-6]) / closes[-6] * 100 if len(closes) >= 6 else 0

    # Gap pct vs prior close
    gap_pct = (closes[-1] - closes[-2]) / closes[-2] * 100 if len(closes) >= 2 else 0

    # Score components
    signals = []
    score = 30  # base

    # RSI
    if 40 <= rsi_val <= 65:
        score += 15
        signals.append("RSI_OPTIMAL")
    elif rsi_val < 30:
        score += 10
        signals.append("RSI_OVERSOLD")
    elif rsi_val > 70:
        score += 5
        signals.append("RSI_OVERBOUGHT")

    # Volume
    if vol_ratio >= 2.0:
        score += 20
        signals.append("VOLUME_SURGE")
    elif vol_ratio >= 1.5:
        score += 12
        signals.append("VOLUME_ELEVATED")
    elif vol_ratio >= 1.2:
        score += 5

    # Momentum
    if mom_5d >= 3:
        score += 15
        signals.append("BULL_MOMENTUM")
    elif mom_5d >= 1:
        score += 8
    elif mom_5d <= -3:
        score += 5
        signals.append("BEAR_MOMENTUM")

    # Gap
    if abs(gap_pct) >= 2:
        score += 10
        signals.append("GAP_UP" if gap_pct > 0 else "GAP_DOWN")
    elif abs(gap_pct) >= 1:
        score += 5

    return min(100, max(0, score)), signals


def compute_levels(df_sym, price):
    """ATR-based trade levels."""
    if len(df_sym) < 14:
        risk = price * RISK_PCT
        atr = risk / STOP_MULT
    else:
        hi = df_sym["High"].values[-14:]
        lo = df_sym["Low"].values[-14:]
        cl = df_sym["Close"].values[-14:]
        tr = np.maximum(hi - lo, np.maximum(abs(hi - np.roll(cl, 1)), abs(lo - np.roll(cl, 1))))
        atr = float(np.mean(tr[1:]))
    risk = atr * 1.5
    stop   = round(price - risk * STOP_MULT, 2)
    tp1    = round(price + risk * TP1_MULT, 2)
    tp2    = round(price + risk * TP2_MULT, 2)

    # Support / resistance from 10-day range
    if len(df_sym) >= 10:
        support    = round(float(df_sym["Low"].values[-10:].min()), 2)
        resistance = round(float(df_sym["High"].values[-10:].max()), 2)
    else:
        support    = round(price * 0.97, 2)
        resistance = round(price * 1.03, 2)

    tp3 = resistance
    entry_lo = round(price - atr * 0.3, 2)
    entry_hi = round(price + atr * 0.1, 2)
    return {
        "atr": round(atr, 2),
        "stop": stop,
        "tp1": tp1,
        "tp2": tp2,
        "tp3": tp3,
        "entry_lo": entry_lo,
        "entry_hi": entry_hi,
        "support": support,
        "resistance": resistance,
    }


def detect_regime(spy_df, as_of_idx):
    """
    Detect market regime using SPY vs 8/21 MA.
    Returns: BULL_STRONG, BULL, NEUTRAL, BEAR, BEAR_STRONG, CRISIS
    """
    if as_of_idx < 21:
        return "NEUTRAL"
    closes = spy_df["Close"].values[:as_of_idx + 1]
    price = closes[-1]
    ma8  = float(np.mean(closes[-8:]))
    ma21 = float(np.mean(closes[-21:]))
    ma50 = float(np.mean(closes[-50:])) if len(closes) >= 50 else ma21

    if price > ma8 > ma21:
        return "BULL_STRONG"
    elif price > ma21:
        return "BULL"
    elif price < ma8 < ma21 and price < ma50 * 0.95:
        return "CRISIS"
    elif price < ma8 < ma21:
        return "BEAR_STRONG"
    elif price < ma21:
        return "BEAR"
    else:
        return "NEUTRAL"


def simulate_day(sym, day_open, day_high, day_low, day_close, levels, entry_style="zone"):
    """
    Simulate one trading day with tiered TP logic.
    entry_style: 'zone' (must touch entry range), 'open' (fill at open)
    Returns: {filled, fill_price, high, low, close, exit_price, exit_reason, pnl}
    """
    entry_lo = levels["entry_lo"]
    entry_hi = levels["entry_hi"]
    stop = levels["stop"]
    tp1  = levels["tp1"]
    tp2  = levels["tp2"]
    tp3  = levels["tp3"]

    # Determine fill price
    if entry_style == "open":
        fill_price = day_open
        filled = True
    else:
        # Filled if daily range touches entry zone
        if day_low <= entry_hi and day_high >= entry_lo:
            # Approximate fill at midpoint of zone or open
            fill_price = round(min(max(entry_lo, day_open), entry_hi), 2)
            filled = True
        else:
            return {
                "filled": False,
                "fill_price": None,
                "high": day_high,
                "low": day_low,
                "close": day_close,
                "exit_price": None,
                "exit_reason": "ZONE_MISS",
                "pnl": 0.0,
            }

    if fill_price <= stop:
        # Gapped through stop
        return {
            "filled": True,
            "fill_price": fill_price,
            "high": day_high,
            "low": day_low,
            "close": day_close,
            "exit_price": stop,
            "exit_reason": "STOP_LOSS",
            "pnl": round((stop - fill_price) / fill_price * TRADE_SIZE, 2),
        }

    qty = TRADE_SIZE / fill_price

    # Tiered TP simulation (simplified — check levels in order)
    # TP1: 50% of position
    # TP2: 25% of remaining (25% total)
    # TP3: remaining 25%
    # Stop: full exit if hit before any TP

    pnl = 0.0
    remaining_qty = qty

    if day_low <= stop:
        # Stop hit
        pnl = (stop - fill_price) * remaining_qty
        return {
            "filled": True,
            "fill_price": fill_price,
            "high": day_high,
            "low": day_low,
            "close": day_close,
            "exit_price": round(stop, 2),
            "exit_reason": "STOP_LOSS",
            "pnl": round(pnl, 2),
        }

    if day_high >= tp1:
        # TP1 hit — exit 50%
        tp1_qty = remaining_qty * 0.5
        pnl += (tp1 - fill_price) * tp1_qty
        remaining_qty -= tp1_qty

        if day_high >= tp2:
            # TP2 hit — exit 25%
            tp2_qty = qty * 0.25
            pnl += (tp2 - fill_price) * tp2_qty
            remaining_qty -= tp2_qty

            if day_high >= tp3:
                # TP3 hit
                pnl += (tp3 - fill_price) * remaining_qty
                return {
                    "filled": True, "fill_price": fill_price,
                    "high": day_high, "low": day_low, "close": day_close,
                    "exit_price": round(tp3, 2), "exit_reason": "TP3",
                    "pnl": round(pnl, 2),
                }
            else:
                # EOD exit on remaining 25%
                pnl += (day_close - fill_price) * remaining_qty
                return {
                    "filled": True, "fill_price": fill_price,
                    "high": day_high, "low": day_low, "close": day_close,
                    "exit_price": round(day_close, 2), "exit_reason": "TP2+EOD",
                    "pnl": round(pnl, 2),
                }
        else:
            # TP1 only, EOD rest
            pnl += (day_close - fill_price) * remaining_qty
            return {
                "filled": True, "fill_price": fill_price,
                "high": day_high, "low": day_low, "close": day_close,
                "exit_price": round(day_close, 2), "exit_reason": "TP1+EOD",
                "pnl": round(pnl, 2),
            }
    else:
        # No TP hit — EOD exit
        pnl = (day_close - fill_price) * remaining_qty
        return {
            "filled": True, "fill_price": fill_price,
            "high": day_high, "low": day_low, "close": day_close,
            "exit_price": round(day_close, 2), "exit_reason": "EOD",
            "pnl": round(pnl, 2),
        }


def setup_db():
    conn = sqlite3.connect(DB_PATH)
    conn.execute("""
        CREATE TABLE IF NOT EXISTS ollie_backtest_30d (
            id          INTEGER PRIMARY KEY AUTOINCREMENT,
            run_date    TEXT,
            trade_date  TEXT,
            symbol      TEXT,
            grade       TEXT,
            score       INTEGER,
            regime      TEXT,
            entry_lo    REAL,
            entry_hi    REAL,
            filled      INTEGER,
            fill_price  REAL,
            stop        REAL,
            tp1         REAL,
            tp2         REAL,
            tp3         REAL,
            day_high    REAL,
            day_low     REAL,
            exit_price  REAL,
            exit_reason TEXT,
            pnl         REAL,
            signals     TEXT
        )
    """)
    conn.commit()
    return conn


def get_trading_days(start: date, end: date) -> list:
    """Return trading days (Mon-Fri, exclude major US holidays)."""
    US_HOLIDAYS_2026 = {
        date(2026, 1, 1), date(2026, 1, 19), date(2026, 2, 16),
        date(2026, 4, 3),  # Good Friday
        date(2026, 5, 25), date(2026, 7, 3), date(2026, 9, 7),
        date(2026, 11, 26), date(2026, 12, 25),
    }
    days = []
    d = start
    while d <= end:
        if d.weekday() < 5 and d not in US_HOLIDAYS_2026:
            days.append(d)
        d += timedelta(days=1)
    return days


def main():
    print("=" * 72)
    print("OLLIE SUPER TRADER — 30-DAY BACKTEST")
    print(f"Period: {START} → {END}")
    print(f"Universe: {', '.join(SYMBOLS)}")
    print("=" * 72)

    trading_days = get_trading_days(START, END)
    print(f"Trading days in range: {len(trading_days)}")

    # Download OHLCV data — go back an extra 60 days for warmup
    warmup_start = START - timedelta(days=90)
    print(f"\nDownloading OHLCV data from {warmup_start} → {END}...")

    all_syms = SYMBOLS + [REGIME_SYMBOL]
    try:
        raw = yf.download(all_syms, start=warmup_start.strftime("%Y-%m-%d"),
                          end=(END + timedelta(days=1)).strftime("%Y-%m-%d"),
                          auto_adjust=True, progress=False, group_by="ticker")
    except Exception as e:
        print(f"Error downloading data: {e}")
        sys.exit(1)

    # Unpack multi-symbol download
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
                df = yf.download(sym, start=warmup_start.strftime("%Y-%m-%d"),
                                 end=(END + timedelta(days=1)).strftime("%Y-%m-%d"),
                                 auto_adjust=True, progress=False)
                data[sym] = df.dropna(subset=["Close"])
            except Exception as ex:
                print(f"  WARNING: Could not load {sym}: {ex}")
                data[sym] = pd.DataFrame()

    spy_df = data.get(REGIME_SYMBOL, pd.DataFrame())

    # ── Run backtest ──────────────────────────────────────────────────────────
    conn = setup_db()
    run_date = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

    all_trades = []
    daily_pnl  = {}

    print("\n{:<12} {:>8} {:>7} {:>7} {:>8} {:>8}  {}".format(
        "DATE", "REGIME", "TRADES", "FILLS", "WINNERS", "PNL", "NOTES"))
    print("-" * 72)

    for trade_date in trading_days:
        date_str = trade_date.strftime("%Y-%m-%d")

        # Get SPY data up to this date for regime
        spy_idx_mask = spy_df.index.date <= trade_date if len(spy_df) > 0 else []
        spy_sub = spy_df[spy_idx_mask] if len(spy_df) > 0 else pd.DataFrame()
        regime = detect_regime(spy_sub, len(spy_sub) - 1) if len(spy_sub) >= 21 else "UNKNOWN"

        # Skip CRISIS/BEAR days
        skip_regime = regime in ("CRISIS", "BEAR_STRONG")

        day_trades = []
        day_fills  = 0
        day_winners = 0
        day_pnl    = 0.0

        for sym in SYMBOLS:
            df_sym = data.get(sym, pd.DataFrame())
            if len(df_sym) < 20:
                continue

            # Get data UP TO but not including trade_date (for scoring/levels)
            prior_mask = df_sym.index.date < trade_date
            df_prior   = df_sym[prior_mask]
            if len(df_prior) < 15:
                continue

            # Get trade_date candle
            today_mask = df_sym.index.date == trade_date
            df_today   = df_sym[today_mask]
            if len(df_today) == 0:
                continue  # market closed or no data

            day_open  = float(df_today["Open"].iloc[0])
            day_high  = float(df_today["High"].iloc[0])
            day_low   = float(df_today["Low"].iloc[0])
            day_close = float(df_today["Close"].iloc[0])
            price     = float(df_prior["Close"].iloc[-1])

            # Score
            score, signals = score_symbol(df_prior)
            scaled = score  # already 0-100
            grade = "A" if scaled >= 80 else "B" if scaled >= 65 else "C" if scaled >= 50 else "D" if scaled >= 35 else "E"

            # Grade gate
            if scaled < GRADE_B_MIN:
                continue

            # Regime gate
            if skip_regime:
                continue
            if regime in ("BEAR",) and grade not in ("A",):
                continue  # only A-grade trades in BEAR

            # Levels
            lvls = compute_levels(df_prior, price)

            # Simulate
            result = simulate_day(sym, day_open, day_high, day_low, day_close, lvls)

            pnl = result["pnl"]
            day_pnl += pnl

            trade_row = {
                "run_date":    run_date,
                "trade_date":  date_str,
                "symbol":      sym,
                "grade":       grade,
                "score":       scaled,
                "regime":      regime,
                "entry_lo":    lvls["entry_lo"],
                "entry_hi":    lvls["entry_hi"],
                "filled":      1 if result["filled"] else 0,
                "fill_price":  result["fill_price"],
                "stop":        lvls["stop"],
                "tp1":         lvls["tp1"],
                "tp2":         lvls["tp2"],
                "tp3":         lvls["tp3"],
                "day_high":    day_high,
                "day_low":     day_low,
                "exit_price":  result["exit_price"],
                "exit_reason": result["exit_reason"],
                "pnl":         pnl,
                "signals":     json.dumps(signals),
            }
            day_trades.append(trade_row)
            all_trades.append(trade_row)

            if result["filled"]:
                day_fills += 1
                if pnl > 0:
                    day_winners += 1

        # Save day trades to DB
        for t in day_trades:
            conn.execute("""INSERT INTO ollie_backtest_30d
                (run_date,trade_date,symbol,grade,score,regime,entry_lo,entry_hi,
                 filled,fill_price,stop,tp1,tp2,tp3,day_high,day_low,exit_price,exit_reason,pnl,signals)
                VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)""",
                (t["run_date"],t["trade_date"],t["symbol"],t["grade"],t["score"],t["regime"],
                 t["entry_lo"],t["entry_hi"],t["filled"],t["fill_price"],t["stop"],
                 t["tp1"],t["tp2"],t["tp3"],t["day_high"],t["day_low"],
                 t["exit_price"],t["exit_reason"],t["pnl"],t["signals"]))

        conn.commit()
        daily_pnl[date_str] = day_pnl

        note = "(CRISIS/SKIP)" if skip_regime else ""
        print("{:<12} {:>8} {:>7} {:>7} {:>8} {:>+8.2f}  {}".format(
            date_str, regime, len(day_trades), day_fills,
            day_winners, day_pnl, note))

    conn.close()

    # ── Summary Stats ─────────────────────────────────────────────────────────
    filled_trades = [t for t in all_trades if t["filled"]]
    winners = [t for t in filled_trades if t["pnl"] > 0]
    losers  = [t for t in filled_trades if t["pnl"] <= 0]
    total_pnl  = sum(t["pnl"] for t in filled_trades)
    win_rate   = len(winners) / max(len(filled_trades), 1) * 100

    # Max drawdown
    cum_pnl = 0
    peak = 0
    max_dd = 0
    for t in filled_trades:
        cum_pnl += t["pnl"]
        if cum_pnl > peak:
            peak = cum_pnl
        dd = peak - cum_pnl
        if dd > max_dd:
            max_dd = dd

    # Daily PnL for Sharpe
    daily_vals = list(daily_pnl.values())
    if len(daily_vals) > 1:
        mean_d = np.mean(daily_vals)
        std_d  = np.std(daily_vals)
        sharpe = (mean_d / std_d * np.sqrt(252)) if std_d > 0 else 0
    else:
        sharpe = 0

    best_day  = max(daily_pnl.items(), key=lambda x: x[1]) if daily_pnl else ("—", 0)
    worst_day = min(daily_pnl.items(), key=lambda x: x[1]) if daily_pnl else ("—", 0)

    # Exit reason breakdown
    reasons = {}
    for t in filled_trades:
        r = t["exit_reason"]
        reasons[r] = reasons.get(r, 0) + 1

    print("\n" + "=" * 72)
    print("SUMMARY STATISTICS")
    print("=" * 72)
    print(f"  Period:           {START} → {END} ({len(trading_days)} trading days)")
    print(f"  Universe:         {len(SYMBOLS)} symbols")
    print(f"  Total signals:    {len(all_trades)}")
    print(f"  Filled trades:    {len(filled_trades)}")
    print(f"  Winners:          {len(winners)}")
    print(f"  Losers:           {len(losers)}")
    print(f"  Win rate:         {win_rate:.1f}%")
    print(f"  Total P&L:        ${total_pnl:+.2f}")
    print(f"  Best day:         {best_day[0]}  ${best_day[1]:+.2f}")
    print(f"  Worst day:        {worst_day[0]}  ${worst_day[1]:+.2f}")
    print(f"  Max drawdown:     ${max_dd:.2f}")
    print(f"  Annualized Sharpe:{sharpe:.2f}")
    print(f"\nExit reason breakdown:")
    for r, cnt in sorted(reasons.items(), key=lambda x: -x[1]):
        print(f"  {r:<20} {cnt:>4} trades")

    # ── Full Trade Table ───────────────────────────────────────────────────────
    print("\n" + "=" * 72)
    print("FULL TRADE-BY-TRADE TABLE (filled only)")
    print("=" * 72)
    hdr = "{:<12} {:<6} {:<5} {:<5} {:<8} {:<9} {:<9} {:<9} {:<9} {:<9} {:<9} {:<9} {:<15} {:>8}"
    print(hdr.format("DATE","SYM","GR","SCR","REGIME",
                     "FILL","STOP","TP1","TP2","HIGH","LOW","EXIT","REASON","PNL"))
    print("-" * 140)
    row_fmt = "{:<12} {:<6} {:<5} {:<5} {:<8} {:<9} {:<9} {:<9} {:<9} {:<9} {:<9} {:<9} {:<15} {:>+8.2f}"
    for t in filled_trades:
        print(row_fmt.format(
            t["trade_date"], t["symbol"], t["grade"], t["score"], t["regime"],
            f"${t['fill_price']:.2f}" if t["fill_price"] else "—",
            f"${t['stop']:.2f}",
            f"${t['tp1']:.2f}",
            f"${t['tp2']:.2f}",
            f"${t['day_high']:.2f}",
            f"${t['day_low']:.2f}",
            f"${t['exit_price']:.2f}" if t["exit_price"] else "—",
            t["exit_reason"],
            t["pnl"],
        ))

    print(f"\nResults saved to trader.db → ollie_backtest_30d ({len(all_trades)} rows total)")


if __name__ == "__main__":
    main()
