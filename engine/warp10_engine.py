"""Warp 10 — Regime-Switching Allocation Engine.

Automatically shifts allocation between Long Equity, Bear Call Spreads,
Iron Condors, and Short Equity based on VIX + SPY trend regime.
Uses proven backtest data for each weapon system.
"""
from __future__ import annotations
import math
import numpy as np
from datetime import datetime, timedelta
from rich.console import Console

console = Console()

# ── Regime-based allocation tables (proven by Arsenal V2 backtests) ──
REGIME_ALLOCATIONS = {
    "BULL": {
        "long_equity": 0.70,
        "bear_call_spreads": 0.10,
        "iron_condors": 0.05,
        "short_equity": 0.00,
        "cash": 0.15,
        "description": "Full offense — phasers at maximum",
    },
    "CAUTIOUS": {
        "long_equity": 0.50,
        "bear_call_spreads": 0.15,
        "iron_condors": 0.10,
        "short_equity": 0.05,
        "cash": 0.20,
        "description": "Yellow alert — balanced offense and defense",
    },
    "BEAR": {
        "long_equity": 0.20,
        "bear_call_spreads": 0.25,
        "iron_condors": 0.15,
        "short_equity": 0.15,
        "cash": 0.25,
        "description": "Red alert — shields up, torpedoes armed",
    },
    "CRISIS": {
        "long_equity": 0.05,
        "bear_call_spreads": 0.15,
        "iron_condors": 0.05,
        "short_equity": 0.25,
        "cash": 0.50,
        "description": "Battlestations — all hands brace for impact",
    },
}

STARTING_CASH = 10000.0

# Tweak 3: Regime transition boost
TRANSITION_BOOST_EQUITY = 0.15   # +15% equity on regime upgrade
TRANSITION_DEFENSE_EQUITY = -0.15  # -15% equity on regime downgrade
TRANSITION_DURATION = 5  # days
REGIME_ORDER = {"CRISIS": 0, "BEAR": 1, "CAUTIOUS": 2, "BULL": 3}


def _classify_regime(vix, spy_price, sma200, sma50):
    """Classify regime from VIX + SPY data."""
    above_200 = spy_price > sma200 if sma200 else True
    above_50 = spy_price > sma50 if sma50 else True

    if vix >= 35:
        return "CRISIS"
    elif vix >= 25 and not above_200:
        return "BEAR"
    elif vix >= 25:
        return "BEAR"
    elif not above_200 and not above_50:
        return "BEAR"
    elif vix >= 20:
        return "CAUTIOUS"
    elif not above_200:
        return "CAUTIOUS"
    else:
        return "BULL"


def get_current_allocation() -> dict:
    """Get the allocation table based on current live market regime."""
    try:
        from engine.regime_detector import detect_regime
        regime = detect_regime()
        vix = regime.get("vix", 20)
        spy = regime.get("spy_price", 0)
        spy_vs_200 = regime.get("spy_vs_200ma", 0)
        spy200 = spy / (1 + spy_vs_200 / 100) if spy and spy_vs_200 else spy
        spy_vs_50 = regime.get("spy_vs_50ma", 0)
        spy50 = spy / (1 + spy_vs_50 / 100) if spy and spy_vs_50 else spy

        r = _classify_regime(vix, spy, spy200, spy50)
        # Map existing regime_detector names to Warp 10 allocation names
        raw_regime = regime.get("regime", "")
        regime_map = {"CRASH_MODE": "CRISIS", "BEAR_TREND": "BEAR", "BULL_TREND": "BULL",
                       "MELT_UP": "BULL", "CHOPPY": "CAUTIOUS"}
        r = regime_map.get(raw_regime, r)
        alloc = REGIME_ALLOCATIONS.get(r, REGIME_ALLOCATIONS["CAUTIOUS"])
        return {
            "regime": r,  # Warp 10 regime name (BULL/CAUTIOUS/BEAR/CRISIS)
            "vix": vix,
            "spy_price": spy,
            "spy_above_200": spy_vs_200 > 0 if spy_vs_200 is not None else True,
            "allocation": alloc,
        }
    except Exception as e:
        return {
            "regime": "CAUTIOUS",
            "vix": 20,
            "allocation": REGIME_ALLOCATIONS["CAUTIOUS"],
            "error": str(e),
        }


# ── Backtest helpers (reuse from arsenal_backtest) ──

def _norm_cdf(x):
    return 0.5 * (1 + math.erf(x / math.sqrt(2)))

def _bs(S, K, T, r, sigma, ot="call"):
    if T <= 0: return max(S-K, 0) if ot == "call" else max(K-S, 0)
    if sigma <= 0: sigma = 0.01
    d1 = (math.log(S/K) + (r + sigma**2/2)*T) / (sigma*math.sqrt(T))
    d2 = d1 - sigma*math.sqrt(T)
    if ot == "call": return S*_norm_cdf(d1) - K*math.exp(-r*T)*_norm_cdf(d2)
    return K*math.exp(-r*T)*_norm_cdf(-d2) - S*_norm_cdf(-d1)

def _hv(c, period=30):
    if len(c) < period+1: return 0.30
    rets = np.diff(np.log(c[-period-1:])).astype(float)
    return float(np.std(rets) * math.sqrt(252) * 1.3)

def _ema(data, span):
    alpha = 2/(span+1); r = [float(data[0])]
    for i in range(1, len(data)): r.append(alpha*float(data[i])+(1-alpha)*r[-1])
    return np.array(r)

def _rsi(c, p=14):
    if len(c)<p+1: return 50.0
    d=np.diff(c); g=np.where(d>0,d,0); l=np.where(d<0,-d,0)
    return 100-(100/(1+np.mean(g[-p:])/np.mean(l[-p:]))) if np.mean(l[-p:])>0 else 100.0

def _atr(h, l, c, p=14):
    if len(h)<p+1: return float(np.mean(np.abs(np.diff(c[-p:]))))*1.5 if len(c)>=p else float(c[-1])*0.02
    tr=[]
    for i in range(-p,0): tr.append(max(float(h[i])-float(l[i]), abs(float(h[i])-float(c[i-1])), abs(float(l[i])-float(c[i-1]))))
    return sum(tr)/len(tr)

def _trail(gain):
    if gain>=0.20: return 0.10
    elif gain>=0.10: return 0.12
    elif gain>=0.05: return 0.15
    else: return 0.05


# ── Long/Short signal detection ──

def _candle_patterns(o, h, l, c):
    """Detect top candlestick patterns from OHLC arrays. Returns list of (name, direction)."""
    pats = []
    if len(c) < 3 or len(o) < 3:
        return pats
    # Current candle
    co, ch, cl, cc = float(o[-1]), float(h[-1]), float(l[-1]), float(c[-1])
    body = abs(cc - co)
    rng = ch - cl
    if rng <= 0:
        return pats
    # Previous candle
    po, pc = float(o[-2]), float(c[-2])
    # Two ago
    ppo, ppc = float(o[-3]), float(c[-3])

    # Hammer (bullish)
    if cc > co and (min(co, cc) - cl) >= 2 * body and (ch - max(co, cc)) <= body * 0.3:
        pats.append(("HAMMER", "BULL"))
    # Shooting star (bearish)
    if cc < co and (ch - max(co, cc)) >= 2 * body and (min(co, cc) - cl) <= body * 0.3:
        pats.append(("SHOOT_STAR", "BEAR"))
    # Doji (neutral/reversal)
    if body <= rng * 0.1:
        pats.append(("DOJI", "NEUTRAL"))
    # Bullish engulfing
    if pc < po and cc > co and co <= pc and cc >= po:
        pats.append(("BULL_ENGULF", "BULL"))
    # Bearish engulfing
    if pc > po and cc < co and co >= pc and cc <= po:
        pats.append(("BEAR_ENGULF", "BEAR"))
    # Morning star (3-candle bullish reversal)
    if ppc < ppo and abs(pc - po) <= (float(h[-2]) - float(l[-2])) * 0.3 and cc > co and cc > (ppo + ppc) / 2:
        pats.append(("MORN_STAR", "BULL"))
    # Three white soldiers
    if cc > co and pc > po and ppc > ppo and cc > pc > ppc:
        pats.append(("3_SOLDIERS", "BULL"))
    # Three black crows
    if cc < co and pc < po and ppc < ppo and cc < pc < ppc:
        pats.append(("3_CROWS", "BEAR"))
    return pats


def _mtf_aligned(c, weekly_c):
    """Multi-timeframe alignment check. Returns 'BULL', 'BEAR', or None."""
    if len(c) < 50 or weekly_c is None or len(weekly_c) < 20:
        return None
    # Daily: price vs 20-SMA + RSI
    d_sma = float(np.mean(c[-20:]))
    d_up = float(c[-1]) > d_sma
    d_rsi = _rsi(c)
    # Weekly: price vs 10-period SMA + RSI
    w_sma = float(np.mean(weekly_c[-10:]))
    w_up = float(weekly_c[-1]) > w_sma
    w_rsi = _rsi(weekly_c)
    if d_up and w_up and d_rsi < 70 and w_rsi < 70:
        return "BULL"
    if not d_up and not w_up:
        return "BEAR"
    return None


def _bonus_score(c, h, l, o, weekly_c):
    """Build 2: Bonus convergence score from candlestick + MTF. ADDITIVE only."""
    bonus = 0.0
    details = []
    # Candlestick patterns: +0.5 each
    if o is not None and len(o) >= 3:
        pats = _candle_patterns(o, h, l, c)
        for name, direction in pats:
            if direction == "BULL":
                bonus += 0.5
                details.append(name)
    # Multi-timeframe: +1.0 if aligned, -0.5 if misaligned
    if weekly_c is not None:
        mtf = _mtf_aligned(c, weekly_c)
        if mtf == "BULL":
            bonus += 1.0
            details.append("MTF_BULL")
        elif mtf == "BEAR":
            bonus -= 0.5
            details.append("MTF_BEAR")
    return bonus, details


def _nearest_support(l, c):
    """Find nearest support level from swing lows."""
    if len(l) < 10:
        return None
    current = float(c[-1])
    swing_lows = []
    for i in range(2, min(len(l) - 2, 60)):
        if float(l[-i]) < float(l[-i-1]) and float(l[-i]) < float(l[-i-2]) and float(l[-i]) < float(l[-i+1]) and float(l[-i]) < float(l[-i+2]):
            if float(l[-i]) < current:
                swing_lows.append(float(l[-i]))
    return max(swing_lows) if swing_lows else None


def _long_sigs(c, h, l, v, av, spy_c, earnings_mode=False, o=None, weekly_c=None):
    """earnings_mode adds earnings signals. o=Open array for candles. weekly_c for MTF."""
    t = []
    if len(c)<55: return t
    sma20=float(np.mean(c[-20:])); sma50=float(np.mean(c[-50:]))
    if float(c[-1])>float(np.max(h[-21:-1])) and float(v[-1])>av*2: t.append("BV")
    if float(c[-1])>sma50 and abs(float(c[-1])-sma20)/float(c[-1])<0.02: t.append("PB")
    if _rsi(c)>30 and _rsi(c[:-1])<30: t.append("RB")
    if len(c)>=27:
        e12=_ema(c,12);e26=_ema(c,26);m=e12-e26;s=_ema(m,9)
        e12p=_ema(c[:-1],12);e26p=_ema(c[:-1],26);mp=e12p-e26p;sp=_ema(mp,9)
        if float(m[-1])>float(s[-1]) and float(mp[-1])<=float(sp[-1]): t.append("MC")
    std20=float(np.std(c[-20:]));bb=sma20-2*std20
    if float(c[-2])<=bb and float(c[-1])>bb: t.append("BB")
    emas=[float(_ema(c,s)[-1]) for s in (8,13,21,34,55)]
    if all(emas[i]>emas[i+1] for i in range(len(emas)-1)): t.append("ER")
    if float(c[-1])>sma20 and float(np.min(c[-5:]))<sma20: t.append("TR")
    if spy_c is not None and len(spy_c)>=50:
        n=min(len(c),len(spy_c)); rs=np.array(c[-n:],dtype=float)/np.array(spy_c[-n:],dtype=float)
        if float(rs[-1])>=float(np.max(rs[:-1])): t.append("RS")
    # Earnings catalyst signals (Warp 10 Final) — these ARE core strategies
    if earnings_mode and len(c) >= 10:
        # Pre-earnings momentum: rising into earnings (last 5 days positive + volume increasing)
        five_day_ret = (float(c[-1]) - float(c[-6])) / float(c[-6]) if len(c) >= 6 else 0
        vol_trend = float(v[-1]) > float(np.mean(v[-5:])) if len(v) >= 5 else False
        if five_day_ret > 0.02 and vol_trend:
            t.append("PRE_EARN")
        # Post-earnings drift: gapped 3%+ recently and still drifting up
        for lookback in range(1, min(8, len(c))):
            gap = (float(c[-lookback]) - float(c[-lookback-1])) / float(c[-lookback-1])
            if gap > 0.03:
                post = (float(c[-1]) - float(c[-lookback])) / float(c[-lookback])
                if post > 0:
                    t.append("POST_DRIFT")
                break
    return t

def _short_sigs(c, h, l, v, av):
    t = []
    if len(c)<55: return t
    if float(c[-1])<float(np.min(l[-21:-1])) and float(v[-1])>av*2: t.append("BD")
    if _rsi(c)<70 and _rsi(c[:-1])>70: t.append("RR")
    if len(h)>=16:
        try:
            hh=[float(np.max(h[i:i+5])) for i in range(-15,0,5)]
            ll=[float(np.min(l[i:i+5])) for i in range(-15,0,5)]
            if hh[2]<hh[1]<hh[0] and ll[2]<ll[1]<ll[0]: t.append("LL")
        except: pass
    if len(h)>=21 and float(c[-2])>float(np.max(h[-22:-2])) and float(c[-1])<float(np.max(h[-22:-2])): t.append("FB")
    if len(c)>=3 and float(c[-2])>float(c[-3]) and float(c[-1])<float(c[-3]): t.append("BE")
    emas=[float(_ema(c,s)[-1]) for s in (8,13,21,34,55)]
    if all(emas[i]<emas[i+1] for i in range(len(emas)-1)): t.append("EB")
    if len(c)>=27:
        e12=_ema(c,12);e26=_ema(c,26);m=e12-e26;s=_ema(m,9)
        e12p=_ema(c[:-1],12);e26p=_ema(c[:-1],26);mp=e12p-e26p;sp=_ema(mp,9)
        if float(m[-1])<float(s[-1]) and float(mp[-1])>=float(sp[-1]): t.append("MD")
    return t


# ── Main Warp 10 Backtest ──

def backtest_warp10(start_date="2026-01-20", end_date="2026-03-21", version="10"):
    """Regime-switching backtest. version='10', '10.5', or 'final'."""
    import yfinance as yf
    from engine.strategy_backtest import _get_tickers

    console.log(f"[bold cyan]Warp 10 Regime-Switch: {start_date} → {end_date}")
    tickers = _get_tickers()
    all_t = list(set(tickers + ["SPY", "^VIX"]))
    dl_s = (datetime.strptime(start_date, "%Y-%m-%d") - timedelta(days=280)).strftime("%Y-%m-%d")
    dl_e = (datetime.strptime(end_date, "%Y-%m-%d") + timedelta(days=35)).strftime("%Y-%m-%d")

    console.log(f"[cyan]Downloading {len(all_t)} tickers...")
    raw = yf.download(all_t, start=dl_s, end=dl_e, group_by="ticker", threads=True, progress=False, auto_adjust=True)

    td = {}
    for t in all_t:
        try:
            df = raw[t].dropna()
            if len(df) >= 20: td[t] = df
        except: pass

    vix_df = td.pop("^VIX", None)
    vix_map = {}
    if vix_df is not None:
        for idx, row in vix_df.iterrows(): vix_map[idx] = float(row["Close"])

    spy_df = td["SPY"]
    dates = sorted(spy_df.index)
    s_dt = datetime.strptime(start_date, "%Y-%m-%d")
    e_dt = datetime.strptime(end_date, "%Y-%m-%d")
    bt = [d for d in dates if s_dt <= d.to_pydatetime().replace(tzinfo=None) <= e_dt]

    console.log(f"[cyan]{len(td)} tickers, {len(bt)} trading days")

    # State
    cash = STARTING_CASH
    long_pos, short_pos = [], []
    long_closed, short_closed, opt_trades = [], [], []
    equity_curve = []
    regime_log = []
    prev_regime = None
    transition_days_left = 0
    transition_type = None  # "UP" or "DOWN"
    recycle_queue = []  # Tweak 4: winner recycling

    for day in bt:
        day_str = day.strftime("%Y-%m-%d")
        spy_mask = spy_df.index <= day
        spy_c = spy_df.loc[spy_mask, "Close"].values

        # VIX
        vix = vix_map.get(day)
        if not vix:
            for off in range(1, 5):
                vix = vix_map.get(day - timedelta(days=off))
                if vix: break
        if not vix: vix = 18.0

        # SPY SMAs
        sma200 = float(np.mean(spy_c[-200:])) if len(spy_c) >= 200 else float(np.mean(spy_c))
        sma50 = float(np.mean(spy_c[-50:])) if len(spy_c) >= 50 else float(np.mean(spy_c))
        spy_px = float(spy_c[-1])

        # Regime
        regime = _classify_regime(vix, spy_px, sma200, sma50)
        alloc = dict(REGIME_ALLOCATIONS[regime])  # copy so we can modify
        regime_log.append(regime)

        # Tweak 3 (10.5): Regime transition boost
        if version == "10.5" and prev_regime is not None and regime != prev_regime:
            cur_lvl = REGIME_ORDER.get(regime, 2)
            prv_lvl = REGIME_ORDER.get(prev_regime, 2)
            if cur_lvl > prv_lvl:
                transition_type = "UP"
                transition_days_left = TRANSITION_DURATION
            elif cur_lvl < prv_lvl:
                transition_type = "DOWN"
                transition_days_left = 3

        if version == "10.5" and transition_days_left > 0:
            if transition_type == "UP":
                alloc["long_equity"] = min(alloc["long_equity"] + TRANSITION_BOOST_EQUITY, 0.85)
                alloc["cash"] = max(alloc["cash"] - TRANSITION_BOOST_EQUITY, 0.05)
            elif transition_type == "DOWN":
                alloc["long_equity"] = max(alloc["long_equity"] + TRANSITION_DEFENSE_EQUITY, 0.05)
                alloc["cash"] = min(alloc["cash"] - TRANSITION_DEFENSE_EQUITY, 0.60)
            transition_days_left -= 1

        prev_regime = regime

        # Budget per strategy (based on current total portfolio value)
        lv = sum(p["qty"] * float(td[p["t"]].loc[td[p["t"]].index <= day, "Close"].iloc[-1])
                 for p in long_pos if p["t"] in td and (td[p["t"]].index <= day).sum() > 0)
        sv = sum((p["e"] - float(td[p["t"]].loc[td[p["t"]].index <= day, "Close"].iloc[-1])) * p["qty"]
                 for p in short_pos if p["t"] in td and (td[p["t"]].index <= day).sum() > 0)
        total_val = cash + lv + sv

        budget_long = total_val * alloc["long_equity"]
        budget_short = total_val * alloc["short_equity"]
        budget_bcs = total_val * alloc["bear_call_spreads"]
        budget_ic = total_val * alloc["iron_condors"]

        # ── Manage LONG positions ──
        still = []
        for pos in long_pos:
            pdf = td.get(pos["t"])
            if pdf is None or (pdf.index <= day).sum() == 0: still.append(pos); continue
            px = float(pdf.loc[pdf.index <= day, "Close"].iloc[-1])
            hi = float(pdf.loc[pdf.index <= day, "High"].iloc[-1])
            if hi > pos["hwm"]: pos["hwm"] = hi
            gain = (px - pos["e"]) / pos["e"]
            if px >= pos["tgt"]:
                pnl = (pos["tgt"]-pos["e"])*pos["qty"]; long_closed.append(_mk(pos, day_str, pos["tgt"], pnl, "TGT", "L")); cash += pos["qty"]*pos["tgt"]; continue
            if gain>0 and pos["hwm"]>pos["e"]:
                tp=_trail(gain); ts=pos["hwm"]*(1-tp)
                if gain>=0.05: ts=max(ts, pos["e"]*0.98)
                if px<=ts: pnl=(ts-pos["e"])*pos["qty"]; long_closed.append(_mk(pos,day_str,ts,pnl,"TRAIL","L")); cash+=pos["qty"]*ts; continue
            if px<=pos["stp"]:
                pnl=(pos["stp"]-pos["e"])*pos["qty"]; long_closed.append(_mk(pos,day_str,pos["stp"],pnl,"STOP","L")); cash+=pos["qty"]*pos["stp"]; continue
            still.append(pos)
        # Tweak 4 (10.5): Winner recycling — allow extra position slot after profitable exit
        recycled = False
        if version == "10.5" and regime in ("BULL", "CAUTIOUS") and len(long_pos) < 5:
            # If we just closed a winner, scan for immediate redeployment
            prev_count = len(still)
            if prev_count < len(long_closed):  # positions were closed
                # Check if any were profitable
                recent_exits = long_closed[-3:]  # check last few
                had_winner = any(t.get("pnl", 0) > 0 for t in recent_exits if t.get("exit_date") == day_str)
                if had_winner:
                    recycled = True  # Will allow extra entry below

        long_pos = still

        # ── Manage SHORT positions ──
        still = []
        for pos in short_pos:
            pdf = td.get(pos["t"])
            if pdf is None or (pdf.index <= day).sum() == 0: still.append(pos); continue
            px = float(pdf.loc[pdf.index <= day, "Close"].iloc[-1])
            if px <= pos["tgt"]:
                pnl=(pos["e"]-pos["tgt"])*pos["qty"]; short_closed.append(_mk(pos,day_str,pos["tgt"],pnl,"TGT","S")); cash+=pos["qty"]*pos["e"]+pnl; continue
            if px >= pos["stp"]:
                pnl=(pos["e"]-pos["stp"])*pos["qty"]; short_closed.append(_mk(pos,day_str,pos["stp"],pnl,"STOP","S")); cash+=pos["qty"]*pos["e"]+pnl; continue
            still.append(pos)
        short_pos = still

        # ── Open LONG positions (if regime allocates equity) ──
        max_long = 5
        if version == "10.5" and recycled:
            max_long = 6  # Allow one extra slot for winner recycling
        if alloc["long_equity"] > 0 and len(long_pos) < max_long:
            held = set(p["t"] for p in long_pos)
            cands = []
            for t, df in td.items():
                if t in ("SPY",) or t in held: continue
                m = df.index <= day
                if m.sum() < 55: continue
                sub = df.loc[m]
                c=sub["Close"].values; h=sub["High"].values; l=sub["Low"].values; v=sub["Volume"].values
                av=float(np.mean(v[-20:])) if len(v)>=20 else float(np.mean(v))
                is_final = version == "final"
                # Core strategies only — no Build 2 signals in convergence check
                trig = _long_sigs(c,h,l,v,av,spy_c, earnings_mode=is_final)
                if len(trig) >= 3:
                    atr_val = _atr(h,l,c)
                    # Build 2: Compute bonus score for ranking (ADDITIVE only)
                    bonus = 0.0
                    bonus_details = []
                    sup = None
                    if is_final:
                        o_arr = sub["Open"].values if "Open" in sub.columns else None
                        w_c = c[::5] if len(c) >= 50 else None
                        bonus, bonus_details = _bonus_score(c, h, l, o_arr, w_c)
                        sup = _nearest_support(l, c)
                    # Ranking score: core count + bonus
                    rank_score = len(trig) + bonus
                    cands.append((t, trig + bonus_details, float(c[-1]), atr_val, sup, rank_score))
            cands.sort(key=lambda x: x[5], reverse=True)  # Sort by rank_score (core + bonus)
            for cand in cands[:3]:
                t, trig, entry, atr = cand[0], cand[1], cand[2], cand[3]
                sup = cand[4] if len(cand) > 4 else None
                if len(long_pos)>=max_long: break
                # Use support level for stop if available and reasonable, else ATR
                atr_stop = round(entry - 2 * atr, 2)
                if sup and sup < entry and (entry - sup) / entry < 0.08:
                    stp = round(max(sup * 0.99, atr_stop), 2)  # Just below support
                else:
                    stp = atr_stop
                tgt=round(entry+3*atr,2)
                if entry-stp<=0: continue
                size=min(budget_long*0.30, cash*0.30); qty=size/entry
                if qty*entry>cash*0.85 or qty<=0: continue
                cash-=qty*entry
                long_pos.append({"t":t,"ed":day_str,"e":entry,"qty":qty,"stp":stp,"tgt":tgt,"hwm":entry,"strats":trig})

        # ── Open SHORT positions (bear/crisis only) ──
        if alloc["short_equity"] > 0 and len(short_pos) < 3:
            held_s = set(p["t"] for p in short_pos)
            s_cands = []
            for t, df in td.items():
                if t in ("SPY",) or t in held_s: continue
                m = df.index <= day
                if m.sum() < 55: continue
                sub = df.loc[m]
                c=sub["Close"].values; h=sub["High"].values; l=sub["Low"].values; v=sub["Volume"].values
                av=float(np.mean(v[-20:])) if len(v)>=20 else float(np.mean(v))
                trig = _short_sigs(c,h,l,v,av)
                if len(trig) >= 3:
                    s_cands.append((t, trig, float(c[-1]), _atr(h,l,c)))
            s_cands.sort(key=lambda x: len(x[1]), reverse=True)
            for t, trig, entry, atr in s_cands[:2]:
                if len(short_pos)>=3: break
                stp=round(entry+2*atr,2); tgt=round(entry-3*atr,2)
                size=min(budget_short*0.40, cash*0.20); qty=size/entry
                if qty<=0: continue
                cash-=qty*entry*0.5
                short_pos.append({"t":t,"ed":day_str,"e":entry,"qty":qty,"stp":stp,"tgt":tgt,"hwm":entry,"strats":trig})

        # ── BEAR CALL SPREADS ──
        if alloc["bear_call_spreads"] > 0 and budget_bcs > 50:
            for t, df in td.items():
                if t=="SPY": continue
                m=df.index<=day
                if m.sum()<55: continue
                sub=df.loc[m]; c=sub["Close"].values; h=sub["High"].values; l=sub["Low"].values; v=sub["Volume"].values
                av=float(np.mean(v[-20:])) if len(v)>=20 else float(np.mean(v))
                trig=_short_sigs(c,h,l,v,av)
                if len(trig)>=3:
                    px=float(c[-1]); iv=_hv(c)
                    future=df.loc[df.index>day]
                    cs=round(px*1.03); cb=round(px*1.08); T=30/365
                    cr=_bs(px,cs,T,0.04,iv,"call")-_bs(px,cb,T,0.04,iv,"call")
                    if cr<=0: continue
                    ml=max((cb-cs)*0.05-cr, cr)
                    # Simulate
                    pnl=cr  # assume credit kept if price stays below sell strike
                    for i in range(min(30, len(future))):
                        fpx=float(future["Close"].iloc[i]); rem=(30-i-1)/365
                        if rem<=0: rem=1/365
                        cv=_bs(fpx,cs,rem,0.04,iv,"call")-_bs(fpx,cb,rem,0.04,iv,"call")
                        p=cr-cv
                        if p>=cr*0.5: pnl=p; break
                        if p<=-ml: pnl=-ml; break
                    else:
                        fp=float(future["Close"].iloc[-1]) if len(future)>0 else px
                        pnl=cr if fp<cs else -ml*0.5
                    opt_trades.append({"t":t,"date":day_str,"type":"BCS","pnl":round(pnl,2)})
                    cash+=pnl
                    break

        # ── IRON CONDORS (VIX > 20) ──
        if alloc["iron_condors"] > 0 and vix > 20 and budget_ic > 50:
            for t, df in td.items():
                if t=="SPY": continue
                m=df.index<=day
                if m.sum()<30: continue
                sub=df.loc[m]; c=sub["Close"].values
                px=float(c[-1]); sma=float(np.mean(c[-20:]))
                if abs(px-sma)/px>0.02: continue
                iv=_hv(c); future=df.loc[df.index>day]
                ps=round(px*0.95); pb=round(px*0.90); ccs=round(px*1.05); ccb=round(px*1.10)
                T=30/365
                cr=(_bs(px,ps,T,0.04,iv,"put")-_bs(px,pb,T,0.04,iv,"put")+_bs(px,ccs,T,0.04,iv,"call")-_bs(px,ccb,T,0.04,iv,"call"))
                if cr<=0: continue
                ml=min((ccs-ps)*0.1, cr*3)
                pnl=cr
                for i in range(min(30, len(future))):
                    fpx=float(future["Close"].iloc[i]); rem=(30-i-1)/365
                    if rem<=0: rem=1/365
                    cv=(_bs(fpx,ps,rem,0.04,iv,"put")-_bs(fpx,pb,rem,0.04,iv,"put")+_bs(fpx,ccs,rem,0.04,iv,"call")-_bs(fpx,ccb,rem,0.04,iv,"call"))
                    p=cr-cv
                    if p>=cr*0.5: pnl=p; break
                    if p<=-ml: pnl=-ml; break
                else:
                    fp=float(future["Close"].iloc[-1]) if len(future)>0 else px
                    pnl=cr if ps<fp<ccs else -ml*0.5
                opt_trades.append({"t":t,"date":day_str,"type":"IC","pnl":round(pnl,2)})
                cash+=pnl
                break

        # Equity curve
        lv2=sum(p["qty"]*float(td[p["t"]].loc[td[p["t"]].index<=day,"Close"].iloc[-1]) for p in long_pos if p["t"] in td and (td[p["t"]].index<=day).sum()>0)
        sv2=sum((p["e"]-float(td[p["t"]].loc[td[p["t"]].index<=day,"Close"].iloc[-1]))*p["qty"] for p in short_pos if p["t"] in td and (td[p["t"]].index<=day).sum()>0)
        equity_curve.append({"date":day_str,"regime":regime,"vix":round(vix,1),"value":round(cash+lv2+sv2,2)})

    # Close remaining
    for pos in long_pos:
        px=float(td[pos["t"]]["Close"].iloc[-1]) if pos["t"] in td else pos["e"]
        pnl=(px-pos["e"])*pos["qty"]; long_closed.append(_mk(pos,end_date,px,pnl,"EOP","L")); cash+=pos["qty"]*px
    for pos in short_pos:
        px=float(td[pos["t"]]["Close"].iloc[-1]) if pos["t"] in td else pos["e"]
        pnl=(pos["e"]-px)*pos["qty"]; short_closed.append(_mk(pos,end_date,px,pnl,"EOP","S")); cash+=pos["qty"]*pos["e"]+pnl

    # SPY comparison
    spy_s=float(spy_df.loc[spy_df.index>=bt[0]].iloc[0]["Close"])
    spy_e=float(spy_df.loc[spy_df.index<=bt[-1]].iloc[-1]["Close"])
    spy_ret=(spy_e-spy_s)/spy_s*100

    # Stats
    all_trades = long_closed + short_closed + opt_trades
    wins = sum(1 for t in all_trades if t.get("pnl",0)>0)
    losses = sum(1 for t in all_trades if t.get("pnl",0)<=0)
    total_pnl = sum(t.get("pnl",0) for t in all_trades)
    final = STARTING_CASH + total_pnl

    long_pnl = sum(t["pnl"] for t in long_closed)
    short_pnl = sum(t["pnl"] for t in short_closed)
    bcs_pnl = sum(t["pnl"] for t in opt_trades if t["type"]=="BCS")
    ic_pnl = sum(t["pnl"] for t in opt_trades if t["type"]=="IC")

    regime_counts = {r: regime_log.count(r) for r in set(regime_log)}

    return {
        "period": f"{start_date} → {end_date}",
        "days": len(bt),
        "tickers": len(td),
        "total_return": round(total_pnl/STARTING_CASH*100, 2),
        "total_pnl": round(total_pnl, 2),
        "final_value": round(final, 2),
        "spy_return": round(spy_ret, 2),
        "alpha": round(total_pnl/STARTING_CASH*100 - spy_ret, 2),
        "trades": wins + losses,
        "wins": wins, "losses": losses,
        "win_rate": round(wins/(wins+losses)*100,1) if wins+losses>0 else 0,
        "long_pnl": round(long_pnl, 2),
        "short_pnl": round(short_pnl, 2),
        "bcs_pnl": round(bcs_pnl, 2),
        "ic_pnl": round(ic_pnl, 2),
        "regime_breakdown": regime_counts,
        "equity_curve": equity_curve,
        "long_trades": long_closed,
        "short_trades": short_closed,
        "opt_trades": opt_trades,
    }


def _mk(pos, exit_date, exit_px, pnl, exit_type, direction):
    return {
        "ticker": pos["t"], "direction": direction,
        "entry_date": pos["ed"], "exit_date": exit_date,
        "entry": round(pos["e"],2), "exit": round(exit_px,2),
        "qty": round(pos["qty"],4), "pnl": round(pnl,2),
        "exit_type": exit_type, "strategies": pos.get("strats",[]),
    }
