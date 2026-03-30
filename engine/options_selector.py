"""Select optimal options expiry and strike for arena AI players."""
from __future__ import annotations
from datetime import datetime, timedelta
from rich.console import Console

console = Console()


def select_option(symbol: str, option_type: str, target_dte: int = 30,
                  min_dte: int = 7) -> dict | None:
    """Pick the best expiry + ATM strike for a symbol.

    Returns {"expiry_date": "YYYY-MM-DD", "strike_price": float} or None.
    """
    try:
        from engine.market_data import yahoo_options_chain
        chain = yahoo_options_chain(symbol)
        if not chain:
            return None

        # Get available expiry dates (Unix timestamps)
        expiry_timestamps = chain.get("expirationDates", [])
        if not expiry_timestamps:
            return None

        # Current price from the chain quote
        quote = chain.get("quote", {})
        current_price = quote.get("regularMarketPrice", 0)
        if not current_price:
            return None

        # Convert timestamps to dates and find nearest to target_dte
        today = datetime.now().date()
        target_date = today + timedelta(days=target_dte)
        min_date = today + timedelta(days=min_dte)

        best_expiry = None
        best_diff = float("inf")
        best_ts = None

        for ts in expiry_timestamps:
            exp_date = datetime.utcfromtimestamp(ts).date()
            if exp_date < min_date:
                continue
            diff = abs((exp_date - target_date).days)
            if diff < best_diff:
                best_diff = diff
                best_expiry = exp_date
                best_ts = ts

        if not best_expiry:
            # Fallback: just pick the first expiry that's >= min_dte
            for ts in sorted(expiry_timestamps):
                exp_date = datetime.utcfromtimestamp(ts).date()
                if exp_date >= min_date:
                    best_expiry = exp_date
                    best_ts = ts
                    break

        if not best_expiry:
            return None

        # Now fetch the chain for that specific expiry to get strikes
        from engine.market_data import yahoo_options_chain_for_date
        dated_chain = yahoo_options_chain_for_date(symbol, best_ts)

        if dated_chain:
            options_list = dated_chain.get("calls" if option_type == "call" else "puts", [])
        else:
            # Fallback: use the default chain's options
            options_data = chain.get("options", [])
            if not options_data:
                return {"expiry_date": best_expiry.isoformat(), "strike_price": round(current_price, 2)}
            first_exp = options_data[0]
            options_list = first_exp.get("calls" if option_type == "call" else "puts", [])

        if not options_list:
            return {"expiry_date": best_expiry.isoformat(), "strike_price": round(current_price, 2)}

        # Find ATM or slightly ITM strike (prefer higher delta / less OTM risk)
        # For calls: ATM or 1 strike ITM (strike <= current_price)
        # For puts:  ATM or 1 strike ITM (strike >= current_price)
        prefer_itm = getattr(config, "OPTIONS_PREFER_ITM", True) if "config" in dir() else True
        try:
            import config as _cfg
            prefer_itm = getattr(_cfg, "OPTIONS_PREFER_ITM", True)
        except Exception:
            pass

        # Target delta 0.30-0.50 (ATM to slightly ITM)
        # For calls: strike slightly below current_price = higher delta
        # For puts:  strike slightly above current_price = higher delta
        # We use strike proximity as a delta proxy (ATM ≈ 0.50 delta)
        best_strike = None
        best_score = float("inf")
        option_premium = None
        best_bid_ask_ok = False

        for opt in options_list:
            strike = opt.get("strike", 0)
            ask = opt.get("ask", 0)
            bid = opt.get("bid", 0)
            iv = opt.get("impliedVolatility", 0)
            delta = opt.get("delta", None)

            # Bid/ask spread filter: skip if spread > 10% of mid
            if ask > 0 and bid > 0:
                mid = (ask + bid) / 2
                spread_pct = (ask - bid) / mid if mid > 0 else 1.0
                if spread_pct > 0.10:
                    continue  # spread too wide
                option_premium_candidate = round(mid, 2)
                bid_ask_ok = True
            else:
                option_premium_candidate = opt.get("lastPrice", 0)
                bid_ask_ok = False

            # Delta targeting: prefer 0.30-0.50 delta
            # If delta not available, use strike proximity as proxy
            if delta is not None:
                delta_val = abs(float(delta))
                # Score: 0 = perfect (0.40 delta), penalty for outside 0.30-0.50
                if 0.30 <= delta_val <= 0.50:
                    delta_score = abs(delta_val - 0.40)
                else:
                    delta_score = abs(delta_val - 0.40) + 0.5  # penalty for out-of-range
            else:
                # Proxy: ATM = best (strike closest to current_price)
                diff = abs(strike - current_price)
                delta_score = diff / current_price if current_price > 0 else diff

                if prefer_itm:
                    if option_type == "call" and strike > current_price * 1.02:
                        delta_score += 0.5
                    elif option_type == "put" and strike < current_price * 0.98:
                        delta_score += 0.5

            # Prefer strikes with good bid/ask
            score = delta_score - (0.1 if bid_ask_ok else 0)

            if score < best_score:
                best_score = score
                best_strike = strike
                option_premium = option_premium_candidate if option_premium_candidate > 0 else None
                best_bid_ask_ok = bid_ask_ok

        if not best_strike:
            best_strike = round(current_price, 2)

        # Earnings warning: flag if earnings within 7 days
        earnings_warning = False
        try:
            import yfinance as _yf2
            _t2 = _yf2.Ticker(symbol)
            _cal = _t2.calendar
            if _cal is not None:
                import pandas as _pd
                if hasattr(_cal, 'get'):
                    _earn_date = _cal.get("Earnings Date")
                elif hasattr(_cal, 'columns'):
                    _earn_dates = _cal.get("Earnings Date", []) if hasattr(_cal, 'get') else []
                    _earn_date = _earn_dates[0] if _earn_dates else None
                else:
                    _earn_date = None
                if _earn_date is not None:
                    if hasattr(_earn_date, '__len__') and len(_earn_date) > 0:
                        _earn_date = _earn_date[0]
                    if hasattr(_earn_date, 'date'):
                        _earn_date = _earn_date.date()
                    _days_to_earn = (getattr(_earn_date, 'date', lambda: _earn_date)() - today).days
                    if 0 <= _days_to_earn <= 7:
                        earnings_warning = True
                        console.log(f"[yellow]⚠ EARNINGS WARNING: {symbol} reports in {_days_to_earn}d — option sizing at risk[/yellow]")
        except Exception:
            pass

        result = {
            "expiry_date": best_expiry.isoformat(),
            "strike_price": round(best_strike, 2),
        }
        if option_premium and option_premium > 0:
            result["premium"] = round(option_premium, 2)
        if earnings_warning:
            result["earnings_warning"] = True

        dte = (best_expiry - today).days
        console.log(f"[dim]Options: {symbol} {option_type.upper()} ${best_strike} exp {best_expiry} ({dte}d){' ⚠EARN' if earnings_warning else ''}[/dim]")
        return result

    except Exception as e:
        console.log(f"[red]Options selector error for {symbol}: {e}")
        return None
