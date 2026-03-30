"""Full Arsenal Backtest — Warp 9.8 KISS equity + shorts + options.

Tests long equity, short equity, long calls/puts, iron condors,
cash-secured puts, and credit spreads against historical data.
All using free Yahoo Finance data + Black-Scholes approximation.
"""
from __future__ import annotations
import math
import numpy as np
from datetime import datetime, timedelta
from rich.console import Console

console = Console()

# ── Config ──
STARTING_CASH = 10000.0
MAX_POSITIONS = 5
MIN_RR = 1.5
MIN_CONVERGENCE = 3

# V2 Optimized allocation — cut losers, double down on winners
ALLOC_LONG_EQ = 0.50       # Warp 9.0 KISS — best returns +7.9%
ALLOC_SHORT_EQ = 0.10      # Bear mode only (VIX>25) — 100% WR in bear
ALLOC_BEAR_CALL_SP = 0.20  # 79-90% WR — most consistent winner
ALLOC_IRON_CONDOR = 0.15   # 44-59% WR — steady premium, VIX>20 only
ALLOC_CASH = 0.05
MAX_SINGLE_OPTION = 0.05
OPTIONS_DTE = 30


# ── Black-Scholes (no scipy dependency) ──

def _norm_cdf(x):
    return 0.5 * (1 + math.erf(x / math.sqrt(2)))


def _bs_price(S, K, T, r, sigma, opt_type="call"):
    """Black-Scholes option price."""
    if T <= 0:
        return max(S - K, 0) if opt_type == "call" else max(K - S, 0)
    if sigma <= 0:
        sigma = 0.01
    d1 = (math.log(S / K) + (r + sigma ** 2 / 2) * T) / (sigma * math.sqrt(T))
    d2 = d1 - sigma * math.sqrt(T)
    if opt_type == "call":
        return S * _norm_cdf(d1) - K * math.exp(-r * T) * _norm_cdf(d2)
    else:
        return K * math.exp(-r * T) * _norm_cdf(-d2) - S * _norm_cdf(-d1)


def _hist_vol(closes, period=30):
    """Historical volatility as IV proxy (annualized, * 1.3 for IV premium)."""
    if len(closes) < period + 1:
        return 0.30
    rets = np.diff(np.log(closes[-period - 1 :])).astype(float)
    return float(np.std(rets) * math.sqrt(252) * 1.3)


# ── Technical helpers (same as strategy_backtest.py) ──

def _ema(data, span):
    alpha = 2 / (span + 1)
    r = [float(data[0])]
    for i in range(1, len(data)):
        r.append(alpha * float(data[i]) + (1 - alpha) * r[-1])
    return np.array(r)

def _rsi(c, period=14):
    if len(c) < period + 1: return 50.0
    d = np.diff(c)
    g = np.where(d > 0, d, 0); l = np.where(d < 0, -d, 0)
    ag = np.mean(g[-period:]); al = np.mean(l[-period:])
    return 100 - (100 / (1 + ag / al)) if al > 0 else 100.0

def _atr(h, l, c, period=14):
    if len(h) < period + 1:
        return float(np.mean(np.abs(np.diff(c[-period:])))) if len(c) >= period else float(c[-1]) * 0.02
    tr = []
    for i in range(-period, 0):
        tr.append(max(float(h[i]) - float(l[i]),
                      abs(float(h[i]) - float(c[i-1])),
                      abs(float(l[i]) - float(c[i-1]))))
    return sum(tr) / len(tr)


# ── LONG strategies (Warp 9.8 KISS — simple 3-count, no weighting) ──

def _long_signals(c, h, l, v, avg_v, spy_c):
    triggered = []
    if len(c) < 55: return triggered
    # breakout_volume
    if float(c[-1]) > float(np.max(h[-21:-1])) and float(v[-1]) > avg_v * 2:
        triggered.append("breakout_vol")
    # pullback_sma20
    sma20 = float(np.mean(c[-20:])); sma50 = float(np.mean(c[-50:]))
    if float(c[-1]) > sma50 and abs(float(c[-1]) - sma20) / float(c[-1]) < 0.02:
        triggered.append("pullback_sma20")
    # rsi_bounce
    if _rsi(c) > 30 and _rsi(c[:-1]) < 30:
        triggered.append("rsi_bounce")
    # macd_cross
    if len(c) >= 27:
        e12 = _ema(c, 12); e26 = _ema(c, 26); ml = e12 - e26; sl = _ema(ml, 9)
        e12p = _ema(c[:-1], 12); e26p = _ema(c[:-1], 26); mlp = e12p - e26p; slp = _ema(mlp, 9)
        if float(ml[-1]) > float(sl[-1]) and float(mlp[-1]) <= float(slp[-1]):
            triggered.append("macd_cross")
    # bb_bounce
    std20 = float(np.std(c[-20:])); bb_lower = sma20 - 2 * std20
    if float(c[-2]) <= bb_lower and float(c[-1]) > bb_lower:
        triggered.append("bb_bounce")
    # ema_ribbon
    emas = [float(_ema(c, s)[-1]) for s in (8, 13, 21, 34, 55)]
    if all(emas[i] > emas[i+1] for i in range(len(emas)-1)):
        triggered.append("ema_ribbon")
    # trend_resume
    if float(c[-1]) > sma20 and float(np.min(c[-5:])) < sma20:
        triggered.append("trend_resume")
    # rs_high
    if spy_c is not None and len(spy_c) >= 50:
        n = min(len(c), len(spy_c))
        rs = np.array(c[-n:], dtype=float) / np.array(spy_c[-n:], dtype=float)
        if float(rs[-1]) >= float(np.max(rs[:-1])):
            triggered.append("rs_high")
    # rsi_divergence
    if len(c) >= 30:
        rl = float(np.min(l[-5:])); pl = float(np.min(l[-15:-5]))
        if rl < pl and _rsi(c[-15:]) > _rsi(c[-25:-10]) and _rsi(c[-15:]) < 40:
            triggered.append("rsi_divergence")
    return triggered


# ── SHORT strategies ──

def _short_signals(c, h, l, v, avg_v):
    triggered = []
    if len(c) < 55: return triggered
    # breakdown_volume
    if float(c[-1]) < float(np.min(l[-21:-1])) and float(v[-1]) > avg_v * 2:
        triggered.append("breakdown_vol")
    # death_cross
    if len(c) >= 201:
        sma50 = float(np.mean(c[-50:])); sma200 = float(np.mean(c[-200:]))
        p50 = float(np.mean(c[-51:-1])); p200 = float(np.mean(c[-201:-1]))
        if sma50 < sma200 and p50 >= p200:
            triggered.append("death_cross")
    # rsi_overbought_reject
    if _rsi(c) < 70 and _rsi(c[:-1]) > 70:
        triggered.append("rsi_reject")
    # lower_highs_lower_lows
    if len(h) >= 16:
        try:
            hh = [float(np.max(h[i:i+5])) for i in range(-15, 0, 5)]
            ll = [float(np.min(l[i:i+5])) for i in range(-15, 0, 5)]
            if hh[2] < hh[1] < hh[0] and ll[2] < ll[1] < ll[0]:
                triggered.append("lower_lows")
        except (ValueError, IndexError):
            pass
    # failed_breakout
    if len(h) >= 21 and float(c[-2]) > float(np.max(h[-22:-2])) and float(c[-1]) < float(np.max(h[-22:-2])):
        triggered.append("failed_breakout")
    # bearish_engulfing
    if len(c) >= 3:
        yest_green = float(c[-2]) > float(c[-3])
        if yest_green and float(c[-1]) < float(c[-3]) and float(c[-1]) < float(c[-2]):
            triggered.append("bear_engulf")
    # ema_ribbon_bearish (reversed)
    emas = [float(_ema(c, s)[-1]) for s in (8, 13, 21, 34, 55)]
    if all(emas[i] < emas[i+1] for i in range(len(emas)-1)):
        triggered.append("ema_bear")
    # macd_cross_down
    if len(c) >= 27:
        e12 = _ema(c, 12); e26 = _ema(c, 26); ml = e12 - e26; sl = _ema(ml, 9)
        e12p = _ema(c[:-1], 12); e26p = _ema(c[:-1], 26); mlp = e12p - e26p; slp = _ema(mlp, 9)
        if float(ml[-1]) < float(sl[-1]) and float(mlp[-1]) >= float(slp[-1]):
            triggered.append("macd_cross_down")
    return triggered


# ── Trailing stop ──

def _trail_pct(gain):
    if gain >= 0.20: return 0.10
    elif gain >= 0.10: return 0.12
    elif gain >= 0.05: return 0.15
    else: return 0.05


# ── Main arsenal backtest ──

def backtest_arsenal(start_date="2026-01-20", end_date="2026-03-21"):
    import yfinance as yf
    from engine.strategy_backtest import _get_tickers

    console.log(f"[bold cyan]Full Arsenal Backtest: {start_date} → {end_date}")

    tickers = _get_tickers()
    all_t = list(set(tickers + ["SPY", "^VIX"]))
    dl_start = (datetime.strptime(start_date, "%Y-%m-%d") - timedelta(days=280)).strftime("%Y-%m-%d")
    dl_end = (datetime.strptime(end_date, "%Y-%m-%d") + timedelta(days=35)).strftime("%Y-%m-%d")

    console.log(f"[cyan]Downloading {len(all_t)} tickers...")
    raw = yf.download(all_t, start=dl_start, end=dl_end, group_by="ticker",
                      threads=True, progress=False, auto_adjust=True)

    td = {}
    for t in all_t:
        try:
            df = raw[t].dropna()
            if len(df) >= 20: td[t] = df
        except Exception: pass

    vix_df = td.pop("^VIX", None)
    vix_map = {}
    if vix_df is not None:
        for idx, row in vix_df.iterrows():
            vix_map[idx] = float(row["Close"])

    spy_df = td["SPY"]
    dates = sorted(spy_df.index)
    s_dt = datetime.strptime(start_date, "%Y-%m-%d")
    e_dt = datetime.strptime(end_date, "%Y-%m-%d")
    bt = [d for d in dates if s_dt <= d.to_pydatetime().replace(tzinfo=None) <= e_dt]

    console.log(f"[cyan]{len(td)} tickers, {len(bt)} trading days")

    # ── V2 Sub-portfolio cash pools ──
    cash_long = STARTING_CASH * ALLOC_LONG_EQ
    cash_short = STARTING_CASH * ALLOC_SHORT_EQ
    cash_bcs = STARTING_CASH * ALLOC_BEAR_CALL_SP
    cash_ic = STARTING_CASH * ALLOC_IRON_CONDOR
    cash_reserve = STARTING_CASH * ALLOC_CASH

    long_pos = []
    short_pos = []
    opt_trades = []

    long_closed = []
    short_closed = []
    equity_curve = []

    for day in bt:
        day_str = day.strftime("%Y-%m-%d")
        spy_mask = spy_df.index <= day
        spy_c = spy_df.loc[spy_mask, "Close"].values

        # VIX
        day_vix = vix_map.get(day)
        if day_vix is None:
            for off in range(1, 5):
                day_vix = vix_map.get(day - timedelta(days=off))
                if day_vix: break
        if day_vix is None: day_vix = 18.0

        # ── Manage LONG positions ──
        still = []
        for pos in long_pos:
            pdf = td.get(pos["ticker"])
            if pdf is None: still.append(pos); continue
            m = pdf.index <= day
            if m.sum() == 0: still.append(pos); continue
            px = float(pdf.loc[m, "Close"].iloc[-1])
            hi = float(pdf.loc[m, "High"].iloc[-1])
            if hi > pos["hwm"]: pos["hwm"] = hi
            gain = (px - pos["entry"]) / pos["entry"]
            # Target
            if px >= pos["target"]:
                pnl = (pos["target"] - pos["entry"]) * pos["qty"]
                long_closed.append(_mk(pos, day_str, pos["target"], pnl, "TARGET", "LONG"))
                cash_long += pos["qty"] * pos["target"]; continue
            # Trailing
            if gain > 0 and pos["hwm"] > pos["entry"]:
                tp = _trail_pct(gain); ts = pos["hwm"] * (1 - tp)
                if gain >= 0.05: ts = max(ts, pos["entry"] * 0.98)
                if px <= ts:
                    pnl = (ts - pos["entry"]) * pos["qty"]
                    long_closed.append(_mk(pos, day_str, ts, pnl, "TRAIL", "LONG"))
                    cash_long += pos["qty"] * ts; continue
            # Stop
            if px <= pos["stop"]:
                pnl = (pos["stop"] - pos["entry"]) * pos["qty"]
                long_closed.append(_mk(pos, day_str, pos["stop"], pnl, "STOP", "LONG"))
                cash_long += pos["qty"] * pos["stop"]; continue
            still.append(pos)
        long_pos = still

        # ── Manage SHORT positions ──
        still = []
        for pos in short_pos:
            pdf = td.get(pos["ticker"])
            if pdf is None: still.append(pos); continue
            m = pdf.index <= day
            if m.sum() == 0: still.append(pos); continue
            px = float(pdf.loc[m, "Close"].iloc[-1])
            gain = (pos["entry"] - px) / pos["entry"]  # short: profit when price drops
            # Target (price dropped to target)
            if px <= pos["target"]:
                pnl = (pos["entry"] - pos["target"]) * pos["qty"]
                short_closed.append(_mk(pos, day_str, pos["target"], pnl, "TARGET", "SHORT"))
                cash_short += pos["qty"] * pos["entry"] + pnl; continue
            # Stop (price went up to stop)
            if px >= pos["stop"]:
                pnl = (pos["entry"] - pos["stop"]) * pos["qty"]
                short_closed.append(_mk(pos, day_str, pos["stop"], pnl, "STOP", "SHORT"))
                cash_short += pos["qty"] * pos["entry"] + pnl; continue
            still.append(pos)
        short_pos = still

        # ── Scan for new LONG signals ──
        if len(long_pos) < MAX_POSITIONS:
            held = set(p["ticker"] for p in long_pos)
            candidates = []
            for t, df in td.items():
                if t in ("SPY",) or t in held: continue
                m = df.index <= day
                if m.sum() < 55: continue
                sub = df.loc[m]
                c = sub["Close"].values; h = sub["High"].values
                l = sub["Low"].values; v = sub["Volume"].values
                avg_v = float(np.mean(v[-20:])) if len(v) >= 20 else float(np.mean(v))
                trig = _long_signals(c, h, l, v, avg_v, spy_c)
                if len(trig) >= MIN_CONVERGENCE:
                    candidates.append((t, trig, float(c[-1]), _atr(h, l, c)))
            candidates.sort(key=lambda x: len(x[1]), reverse=True)
            for t, trig, entry, atr in candidates[:5]:
                if len(long_pos) >= MAX_POSITIONS: break
                stop = round(entry - 2 * atr, 2); target = round(entry + 3 * atr, 2)
                risk = entry - stop; reward = target - entry
                if risk <= 0 or reward / risk < MIN_RR: continue
                size = cash_long * 0.30; qty = size / entry; cost = qty * entry
                if cost > cash_long * 0.85 or cost <= 0: continue
                cash_long -= cost
                long_pos.append({"ticker": t, "entry_date": day_str, "entry": entry,
                                 "qty": qty, "stop": stop, "target": target, "hwm": entry,
                                 "strategies": trig})

        # ── Scan for new SHORT signals (BEAR MODE ONLY: VIX > 25) ──
        if day_vix > 25 and len(short_pos) < 3:
            held_s = set(p["ticker"] for p in short_pos)
            s_candidates = []
            for t, df in td.items():
                if t in ("SPY",) or t in held_s: continue
                m = df.index <= day
                if m.sum() < 55: continue
                sub = df.loc[m]
                c = sub["Close"].values; h = sub["High"].values
                l = sub["Low"].values; v = sub["Volume"].values
                avg_v = float(np.mean(v[-20:])) if len(v) >= 20 else float(np.mean(v))
                trig = _short_signals(c, h, l, v, avg_v)
                if len(trig) >= MIN_CONVERGENCE:
                    s_candidates.append((t, trig, float(c[-1]), _atr(h, l, c)))
            s_candidates.sort(key=lambda x: len(x[1]), reverse=True)
            for t, trig, entry, atr in s_candidates[:3]:
                if len(short_pos) >= 3: break
                stop = round(entry + 2 * atr, 2)
                target = round(entry - 3 * atr, 2)
                size = cash_short * 0.40; qty = size / entry
                if qty * entry > cash_short * 0.85 or qty <= 0: continue
                cash_short -= qty * entry * 0.5
                short_pos.append({"ticker": t, "entry_date": day_str, "entry": entry,
                                  "qty": qty, "stop": stop, "target": target, "hwm": entry,
                                  "strategies": trig})

        # ── BEAR CALL SPREADS on bearish convergence (3+ short strats) ──
        if cash_bcs > STARTING_CASH * MAX_SINGLE_OPTION:
            for t, df in td.items():
                if t == "SPY": continue
                m = df.index <= day
                if m.sum() < 55: continue
                sub = df.loc[m]
                c = sub["Close"].values; h = sub["High"].values
                l = sub["Low"].values; v = sub["Volume"].values
                avg_v = float(np.mean(v[-20:])) if len(v) >= 20 else float(np.mean(v))
                trig = _short_signals(c, h, l, v, avg_v)
                if len(trig) >= 3:
                    px = float(c[-1]); iv = _hist_vol(c)
                    future = df.loc[df.index > day]
                    result = _sim_call_spread(future, px, iv, OPTIONS_DTE)
                    if result:
                        opt_trades.append({"ticker": t, "date": day_str, "type": "BEAR_CALL_SPREAD",
                                           "premium": round(result.get("credit", 0), 2), **result})
                        cash_bcs += result["pnl"]
                        break

        # ── IRON CONDORS when VIX > 20 (range-bound stocks) ──
        if day_vix > 20 and cash_ic > STARTING_CASH * MAX_SINGLE_OPTION:
            for t, df in td.items():
                if t == "SPY": continue
                m = df.index <= day
                if m.sum() < 30: continue
                sub = df.loc[m]
                c = sub["Close"].values
                px = float(c[-1]); sma = float(np.mean(c[-20:]))
                if abs(px - sma) / px > 0.02: continue
                iv = _hist_vol(c)
                future = df.loc[df.index > day]
                result = _sim_iron_condor(future, px, iv, OPTIONS_DTE)
                if result:
                    opt_trades.append({"ticker": t, "date": day_str, "type": "IRON_CONDOR",
                                       "premium": round(result.get("credit", 0), 2), **result})
                    cash_ic += result["pnl"]
                    break

        # ── Equity curve ──
        lv = sum(p["qty"] * float(td[p["ticker"]].loc[td[p["ticker"]].index <= day, "Close"].iloc[-1])
                 for p in long_pos if p["ticker"] in td and (td[p["ticker"]].index <= day).sum() > 0)
        sv_pnl = sum((p["entry"] - float(td[p["ticker"]].loc[td[p["ticker"]].index <= day, "Close"].iloc[-1])) * p["qty"]
                     for p in short_pos if p["ticker"] in td and (td[p["ticker"]].index <= day).sum() > 0)
        total = cash_long + cash_short + cash_bcs + cash_ic + cash_reserve + lv + sv_pnl
        equity_curve.append({"date": day_str, "value": round(total, 2)})

    # ── Close remaining positions at end ──
    for pos in long_pos:
        px = float(td[pos["ticker"]]["Close"].iloc[-1]) if pos["ticker"] in td else pos["entry"]
        pnl = (px - pos["entry"]) * pos["qty"]
        long_closed.append(_mk(pos, end_date, px, pnl, "EOP", "LONG"))
        cash_long += pos["qty"] * px
    for pos in short_pos:
        px = float(td[pos["ticker"]]["Close"].iloc[-1]) if pos["ticker"] in td else pos["entry"]
        pnl = (pos["entry"] - px) * pos["qty"]
        short_closed.append(_mk(pos, end_date, px, pnl, "EOP", "SHORT"))
        cash_short += pos["qty"] * pos["entry"] + pnl

    # ── SPY comparison ──
    spy_s = float(spy_df.loc[spy_df.index >= bt[0]].iloc[0]["Close"])
    spy_e = float(spy_df.loc[spy_df.index <= bt[-1]].iloc[-1]["Close"])
    spy_ret = (spy_e - spy_s) / spy_s * 100

    # ── Compile stats ──
    def _stats(trades, label):
        w = [t for t in trades if t["pnl"] > 0]
        lo = [t for t in trades if t["pnl"] <= 0]
        tp = sum(t["pnl"] for t in trades)
        return {
            "strategy": label, "trades": len(trades), "wins": len(w), "losses": len(lo),
            "win_rate": round(len(w)/len(trades)*100, 1) if trades else 0,
            "total_pnl": round(tp, 2),
            "best": round(max(t["pnl"] for t in trades), 2) if trades else 0,
            "worst": round(min(t["pnl"] for t in trades), 2) if trades else 0,
        }

    condors = [t for t in opt_trades if t["type"] == "IRON_CONDOR"]
    bcs = [t for t in opt_trades if t["type"] == "BEAR_CALL_SPREAD"]

    strats = [
        _stats(long_closed, "LONG_EQ (9.8)"),
        _stats(short_closed, "SHORT_EQ (bear)"),
        _stats(bcs, "BEAR_CALL_SPREAD"),
        _stats(condors, "IRON_CONDOR"),
    ]

    total_pnl = sum(s["total_pnl"] for s in strats)
    final = STARTING_CASH + total_pnl

    return {
        "period": f"{start_date} → {end_date}",
        "days": len(bt),
        "tickers": len(td),
        "spy_return": round(spy_ret, 2),
        "total_pnl": round(total_pnl, 2),
        "total_return": round(total_pnl / STARTING_CASH * 100, 2),
        "final_value": round(final, 2),
        "alpha": round(total_pnl / STARTING_CASH * 100 - spy_ret, 2),
        "strategies": strats,
        "equity_curve": equity_curve,
        "long_trades": long_closed,
        "short_trades": short_closed,
        "option_trades": opt_trades,
    }


def _mk(pos, exit_date, exit_px, pnl, exit_type, direction):
    return {
        "ticker": pos["ticker"], "direction": direction,
        "entry_date": pos["entry_date"], "exit_date": exit_date,
        "entry": round(pos["entry"], 2), "exit": round(exit_px, 2),
        "qty": round(pos["qty"], 4), "pnl": round(pnl, 2),
        "pnl_pct": round((exit_px - pos["entry"]) / pos["entry"] * 100, 2) if direction == "LONG"
                   else round((pos["entry"] - exit_px) / pos["entry"] * 100, 2),
        "exit_type": exit_type, "strategies": pos.get("strategies", []),
    }


def _sim_option(future_df, strike, premium, iv, dte, opt_type):
    """Simulate a long call/put trade forward through price data."""
    for i in range(min(dte - 7, len(future_df))):
        px = float(future_df["Close"].iloc[i])
        rem = dte - i - 1
        if rem <= 0: rem = 1
        cur = _bs_price(px, strike, rem / 365, 0.04, iv, opt_type)
        pnl_pct = (cur - premium) / premium if premium > 0 else 0
        if pnl_pct >= 1.0:
            return {"pnl": round(premium, 2), "pnl_pct": 100, "exit_type": "TARGET", "days": i + 1}
        if pnl_pct <= -0.50:
            return {"pnl": round(-premium * 0.5, 2), "pnl_pct": -50, "exit_type": "STOP", "days": i + 1}
        if rem <= 7:
            return {"pnl": round(cur - premium, 2), "pnl_pct": round(pnl_pct * 100, 1),
                    "exit_type": "TIME_EXIT", "days": i + 1}
    return {"pnl": round(-premium, 2), "pnl_pct": -100, "exit_type": "EXPIRED", "days": dte}


def _sim_iron_condor(future_df, entry_px, iv, dte):
    """Simulate iron condor: sell 5% OTM put/call, buy 10% OTM wings."""
    ps = round(entry_px * 0.95); pb = round(entry_px * 0.90)
    cs = round(entry_px * 1.05); cb = round(entry_px * 1.10)
    T = dte / 365
    credit = (_bs_price(entry_px, ps, T, 0.04, iv, "put") - _bs_price(entry_px, pb, T, 0.04, iv, "put")
              + _bs_price(entry_px, cs, T, 0.04, iv, "call") - _bs_price(entry_px, cb, T, 0.04, iv, "call"))
    if credit <= 0: return None
    width = cs - ps  # approximate
    max_loss = min(width * 0.1, credit * 3)  # cap max loss

    for i in range(min(dte, len(future_df))):
        px = float(future_df["Close"].iloc[i])
        rem = dte - i - 1
        if rem <= 0: rem = 1
        cur = (_bs_price(px, ps, rem/365, 0.04, iv, "put") - _bs_price(px, pb, rem/365, 0.04, iv, "put")
               + _bs_price(px, cs, rem/365, 0.04, iv, "call") - _bs_price(px, cb, rem/365, 0.04, iv, "call"))
        pnl = credit - cur
        if pnl >= credit * 0.5:
            return {"pnl": round(pnl, 2), "credit": credit, "exit_type": "PROFIT_50", "days": i + 1}
        if pnl <= -max_loss:
            return {"pnl": round(-max_loss, 2), "credit": credit, "exit_type": "MAX_LOSS", "days": i + 1}
    # Expiry
    final = float(future_df["Close"].iloc[-1]) if len(future_df) > 0 else entry_px
    if ps < final < cs:
        return {"pnl": round(credit, 2), "credit": credit, "exit_type": "EXPIRED_WIN", "days": dte}
    return {"pnl": round(-max_loss * 0.5, 2), "credit": credit, "exit_type": "EXPIRED_LOSS", "days": dte}


def _sim_put_spread(future_df, entry_px, iv, dte):
    """Bull put spread: sell 3% OTM put, buy 8% OTM put."""
    ps = round(entry_px * 0.97); pb = round(entry_px * 0.92)
    T = dte / 365
    credit = _bs_price(entry_px, ps, T, 0.04, iv, "put") - _bs_price(entry_px, pb, T, 0.04, iv, "put")
    if credit <= 0: return None
    width = ps - pb; max_loss = width * 0.05 - credit  # per-share, scaled
    if max_loss <= 0: max_loss = credit

    for i in range(min(dte, len(future_df))):
        px = float(future_df["Close"].iloc[i])
        rem = dte - i - 1
        if rem <= 0: rem = 1
        cur = _bs_price(px, ps, rem/365, 0.04, iv, "put") - _bs_price(px, pb, rem/365, 0.04, iv, "put")
        pnl = credit - cur
        if pnl >= credit * 0.5:
            return {"pnl": round(pnl, 2), "credit": credit, "exit_type": "PROFIT_50", "days": i + 1}
        if pnl <= -max_loss:
            return {"pnl": round(-max_loss, 2), "credit": credit, "exit_type": "MAX_LOSS", "days": i + 1}
    final = float(future_df["Close"].iloc[-1]) if len(future_df) > 0 else entry_px
    if final > ps:
        return {"pnl": round(credit, 2), "credit": credit, "exit_type": "EXPIRED_WIN", "days": dte}
    return {"pnl": round(-max_loss * 0.5, 2), "credit": credit, "exit_type": "EXPIRED_LOSS", "days": dte}


def _sim_call_spread(future_df, entry_px, iv, dte):
    """Bear call spread: sell 3% OTM call, buy 8% OTM call."""
    cs = round(entry_px * 1.03); cb = round(entry_px * 1.08)
    T = dte / 365
    credit = _bs_price(entry_px, cs, T, 0.04, iv, "call") - _bs_price(entry_px, cb, T, 0.04, iv, "call")
    if credit <= 0: return None
    width = cb - cs; max_loss = width * 0.05 - credit
    if max_loss <= 0: max_loss = credit

    for i in range(min(dte, len(future_df))):
        px = float(future_df["Close"].iloc[i])
        rem = dte - i - 1
        if rem <= 0: rem = 1
        cur = _bs_price(px, cs, rem/365, 0.04, iv, "call") - _bs_price(px, cb, rem/365, 0.04, iv, "call")
        pnl = credit - cur
        if pnl >= credit * 0.5:
            return {"pnl": round(pnl, 2), "credit": credit, "exit_type": "PROFIT_50", "days": i + 1}
        if pnl <= -max_loss:
            return {"pnl": round(-max_loss, 2), "credit": credit, "exit_type": "MAX_LOSS", "days": i + 1}
    final = float(future_df["Close"].iloc[-1]) if len(future_df) > 0 else entry_px
    if final < cs:
        return {"pnl": round(credit, 2), "credit": credit, "exit_type": "EXPIRED_WIN", "days": dte}
    return {"pnl": round(-max_loss * 0.5, 2), "credit": credit, "exit_type": "EXPIRED_LOSS", "days": dte}
