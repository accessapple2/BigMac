"""
TradeMinds UOA Scraper
======================
Pulls options chain data from FREE sources:
  1. yfinance - reliable, free, full options chains
  2. Barchart unofficial API (if installed)

Computes unusual activity signals locally:
  - Vol/OI ratio spikes
  - Big premium bets
  - Put/Call ratio anomalies
  - OTM speculative positioning

Usage:
    from uoa.scraper import UOAScraper
    scraper = UOAScraper(db_path='trader.db')
    results = scraper.scan_watchlist()       # scan Chekov's 528 stocks
    results = scraper.scan_tickers(['META', 'NVDA', 'AAPL'])  # specific tickers
"""

import sqlite3
import time
import traceback
from datetime import datetime, timedelta
from typing import Optional

# yfinance is our primary free source
try:
    import yfinance as yf
    HAS_YFINANCE = True
except ImportError:
    HAS_YFINANCE = False
    print("[UOA] WARNING: yfinance not installed. Run: pip install yfinance --break-system-packages")

# Optional: barchart unofficial API
try:
    import barchart
    HAS_BARCHART = True
except ImportError:
    HAS_BARCHART = False


# ---------------------------------------------------------------------------
# Thresholds — tune these to catch more or fewer alerts
# ---------------------------------------------------------------------------
THRESHOLDS = {
    'vol_oi_ratio_min': 3.0,        # Volume 3x open interest = unusual
    'vol_oi_ratio_high': 10.0,      # 10x = very unusual
    'vol_oi_ratio_critical': 25.0,  # 25x = someone knows something
    'premium_min': 25_000,          # $25K minimum premium to flag
    'premium_high': 100_000,        # $100K = big bet
    'premium_critical': 500_000,    # $500K = whale alert
    'volume_min': 100,              # minimum contract volume to consider
    'oi_min': 10,                   # minimum open interest (skip illiquid)
    'max_dte': 60,                  # only look at options expiring within 60 days
    'otm_pct_speculative': 10.0,    # >10% OTM = speculative bet
    'put_call_ratio_high': 2.5,     # daily P/C ratio above this = bearish flow
    'put_call_ratio_low': 0.4,      # daily P/C ratio below this = bullish flow
}


class UOAScraper:
    """Scrapes options data from free sources and detects unusual activity."""

    def __init__(self, db_path: str = 'trader.db'):
        self.db_path = db_path
        self.scan_date = datetime.now().strftime('%Y-%m-%d')
        self.scan_time = datetime.now().strftime('%H:%M:%S')

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    def scan_tickers(self, tickers: list[str], scan_type: str = 'WATCHLIST') -> dict:
        """
        Scan a list of tickers for unusual options activity.
        Returns dict with flow records, alerts, and summary.
        """
        start = time.time()
        all_flow = []
        all_alerts = []
        all_summaries = []
        errors = []

        print(f"[UOA] Starting {scan_type} scan of {len(tickers)} tickers...")

        for i, ticker in enumerate(tickers):
            try:
                if (i + 1) % 25 == 0:
                    print(f"[UOA]   Progress: {i+1}/{len(tickers)} tickers scanned")

                flow, alerts, summary = self._scan_single_ticker(ticker)
                all_flow.extend(flow)
                all_alerts.extend(alerts)
                if summary:
                    all_summaries.append(summary)

                # Rate limit: yfinance doesn't like rapid-fire requests
                if i % 10 == 9:
                    time.sleep(1)

            except Exception as e:
                errors.append(f"{ticker}: {str(e)}")
                continue

        # Store everything in trader.db
        self._store_flow(all_flow)
        self._store_alerts(all_alerts)
        self._store_summaries(all_summaries)

        duration = time.time() - start

        # Log the scan run
        self._log_scan(scan_type, len(tickers), len(all_flow),
                        len(all_alerts), errors, duration)

        result = {
            'scan_date': self.scan_date,
            'tickers_scanned': len(tickers),
            'contracts_flagged': len(all_flow),
            'alerts_generated': len(all_alerts),
            'critical_alerts': len([a for a in all_alerts if a['severity'] == 'CRITICAL']),
            'high_alerts': len([a for a in all_alerts if a['severity'] == 'HIGH']),
            'errors': len(errors),
            'duration_seconds': round(duration, 1),
            'alerts': all_alerts,
        }

        print(f"[UOA] Scan complete: {result['contracts_flagged']} unusual contracts, "
              f"{result['alerts_generated']} alerts "
              f"({result['critical_alerts']} critical, {result['high_alerts']} high) "
              f"in {result['duration_seconds']}s")

        return result

    def scan_watchlist(self) -> dict:
        """Scan Chekov's full 528-stock watchlist from trader.db."""
        tickers = self._get_chekov_watchlist()
        if not tickers:
            print("[UOA] No watchlist found in trader.db, using default mega-caps")
            tickers = self._get_default_watchlist()
        return self.scan_tickers(tickers, scan_type='FULL')

    def scan_quick(self, top_n: int = 50) -> dict:
        """Quick scan of top N stocks from Chekov's latest universe scan."""
        tickers = self._get_top_universe(top_n)
        if not tickers:
            tickers = self._get_default_watchlist()
        return self.scan_tickers(tickers, scan_type='QUICK')

    def get_latest_alerts(self, severity: str = None, limit: int = 20) -> list[dict]:
        """Retrieve latest UOA alerts from the database."""
        conn = sqlite3.connect(self.db_path)
        conn.row_factory = sqlite3.Row
        query = "SELECT * FROM uoa_alerts ORDER BY created_at DESC"
        params = []
        if severity:
            query = ("SELECT * FROM uoa_alerts WHERE severity = ? "
                     "ORDER BY created_at DESC")
            params = [severity]
        query += f" LIMIT {limit}"
        rows = conn.execute(query, params).fetchall()
        conn.close()
        return [dict(r) for r in rows]

    # ------------------------------------------------------------------
    # Core scanning logic
    # ------------------------------------------------------------------

    def _scan_single_ticker(self, ticker: str) -> tuple[list, list, dict]:
        """Scan one ticker's options chain for unusual activity."""
        if not HAS_YFINANCE:
            return [], [], {}

        stock = yf.Ticker(ticker)
        flow_records = []
        alerts = []

        try:
            underlying_price = stock.fast_info.get('lastPrice', None)
            if not underlying_price:
                hist = stock.history(period='1d')
                if hist.empty:
                    return [], [], {}
                underlying_price = float(hist['Close'].iloc[-1])
        except Exception:
            return [], [], {}

        # Get all expiration dates
        try:
            expirations = stock.options
        except Exception:
            return [], [], {}

        if not expirations:
            return [], [], {}

        total_call_vol = 0
        total_put_vol = 0
        total_call_prem = 0
        total_put_prem = 0
        max_vol_oi = 0
        unusual_count = 0
        all_ivs = []

        for exp_date in expirations:
            # Filter: only look at near-term options
            try:
                exp_dt = datetime.strptime(exp_date, '%Y-%m-%d')
                dte = (exp_dt - datetime.now()).days
                if dte < 0 or dte > THRESHOLDS['max_dte']:
                    continue
            except ValueError:
                continue

            try:
                chain = stock.option_chain(exp_date)
            except Exception:
                continue

            # Process calls
            for _, row in chain.calls.iterrows():
                record = self._process_option_row(
                    row, ticker, 'CALL', exp_date, dte, underlying_price
                )
                if record:
                    flow_records.append(record)
                    total_call_vol += record['volume']
                    total_call_prem += record['premium_total']
                    if record['implied_volatility']:
                        all_ivs.append(record['implied_volatility'])
                    if record['vol_oi_ratio'] and record['vol_oi_ratio'] > max_vol_oi:
                        max_vol_oi = record['vol_oi_ratio']
                    unusual_count += 1

            # Process puts
            for _, row in chain.puts.iterrows():
                record = self._process_option_row(
                    row, ticker, 'PUT', exp_date, dte, underlying_price
                )
                if record:
                    flow_records.append(record)
                    total_put_vol += record['volume']
                    total_put_prem += record['premium_total']
                    if record['implied_volatility']:
                        all_ivs.append(record['implied_volatility'])
                    if record['vol_oi_ratio'] and record['vol_oi_ratio'] > max_vol_oi:
                        max_vol_oi = record['vol_oi_ratio']
                    unusual_count += 1

        # Generate alerts from the flow records
        for record in flow_records:
            alert = self._evaluate_alert(record)
            if alert:
                alerts.append(alert)

        # Check for put/call ratio anomalies at the ticker level
        if total_call_vol > 0:
            pc_ratio = total_put_vol / total_call_vol
            if pc_ratio > THRESHOLDS['put_call_ratio_high']:
                alerts.append({
                    'alert_date': self.scan_date,
                    'alert_time': self.scan_time,
                    'ticker': ticker,
                    'alert_type': 'PUT_WALL',
                    'severity': 'HIGH' if pc_ratio > 4.0 else 'MEDIUM',
                    'contract_type': 'PUT',
                    'strike': None,
                    'expiration': None,
                    'vol_oi_ratio': None,
                    'premium_total': total_put_prem,
                    'underlying_price': underlying_price,
                    'description': (
                        f"Heavy put flow on {ticker}! "
                        f"Put/Call ratio {pc_ratio:.1f}x "
                        f"(put vol {total_put_vol:,} vs call vol {total_call_vol:,}). "
                        f"Total put premium ${total_put_prem:,.0f}."
                    ),
                    'chekov_match': 1,
                    'convergence_score': min(pc_ratio * 15, 100),
                })

        # Build daily summary
        pc_ratio = (total_put_vol / total_call_vol) if total_call_vol > 0 else None
        prem_pc_ratio = (total_put_prem / total_call_prem) if total_call_prem > 0 else None
        summary = {
            'scan_date': self.scan_date,
            'ticker': ticker,
            'total_call_volume': total_call_vol,
            'total_put_volume': total_put_vol,
            'put_call_ratio': pc_ratio,
            'total_call_premium': total_call_prem,
            'total_put_premium': total_put_prem,
            'premium_put_call_ratio': prem_pc_ratio,
            'max_vol_oi_ratio': max_vol_oi,
            'unusual_contracts': unusual_count,
            'avg_iv': sum(all_ivs) / len(all_ivs) if all_ivs else None,
            'underlying_close': underlying_price,
        }

        return flow_records, alerts, summary

    def _process_option_row(self, row, ticker, contract_type, exp_date,
                            dte, underlying_price) -> Optional[dict]:
        """
        Process a single options contract row from yfinance.
        Returns a record dict if it meets minimum thresholds, else None.
        """
        try:
            volume = int(row.get('volume', 0) or 0)
            oi = int(row.get('openInterest', 0) or 0)
            strike = float(row.get('strike', 0))
            last = float(row.get('lastPrice', 0) or 0)
            bid = float(row.get('bid', 0) or 0)
            ask = float(row.get('ask', 0) or 0)
            iv = float(row.get('impliedVolatility', 0) or 0)
        except (ValueError, TypeError):
            return None

        # Skip if below minimum thresholds
        if volume < THRESHOLDS['volume_min']:
            return None
        if oi < THRESHOLDS['oi_min']:
            return None

        vol_oi = volume / oi if oi > 0 else 0
        premium = volume * last * 100

        # Only keep if unusual (vol/oi spike OR big premium)
        is_unusual_ratio = vol_oi >= THRESHOLDS['vol_oi_ratio_min']
        is_big_premium = premium >= THRESHOLDS['premium_min']

        if not (is_unusual_ratio or is_big_premium):
            return None

        # Compute moneyness
        if contract_type == 'CALL':
            pct_otm = ((strike - underlying_price) / underlying_price) * 100
            if pct_otm < -2:
                moneyness = 'ITM'
            elif pct_otm > 2:
                moneyness = 'OTM'
            else:
                moneyness = 'ATM'
        else:  # PUT
            pct_otm = ((underlying_price - strike) / underlying_price) * 100
            if pct_otm < -2:
                moneyness = 'ITM'
            elif pct_otm > 2:
                moneyness = 'OTM'
            else:
                moneyness = 'ATM'

        # Sentiment: calls at ask = bullish, puts at ask = bearish
        if contract_type == 'CALL':
            if ask > 0 and last >= ask * 0.95:
                sentiment = 'BULLISH'
            elif bid > 0 and last <= bid * 1.05:
                sentiment = 'BEARISH'
            else:
                sentiment = 'NEUTRAL'
        else:
            if ask > 0 and last >= ask * 0.95:
                sentiment = 'BEARISH'
            elif bid > 0 and last <= bid * 1.05:
                sentiment = 'BULLISH'
            else:
                sentiment = 'NEUTRAL'

        return {
            'scan_date': self.scan_date,
            'scan_time': self.scan_time,
            'ticker': ticker,
            'contract_type': contract_type,
            'strike': strike,
            'expiration': exp_date,
            'dte': dte,
            'volume': volume,
            'open_interest': oi,
            'vol_oi_ratio': round(vol_oi, 2),
            'last_price': last,
            'bid': bid,
            'ask': ask,
            'implied_volatility': round(iv, 4) if iv else None,
            'premium_total': round(premium, 2),
            'moneyness': moneyness,
            'underlying_price': underlying_price,
            'pct_otm': round(abs(pct_otm), 2),
            'sentiment': sentiment,
            'source': 'yfinance',
        }

    def _evaluate_alert(self, record: dict) -> Optional[dict]:
        """Evaluate a flow record and generate an alert if warranted."""
        vol_oi = record['vol_oi_ratio'] or 0
        premium = record['premium_total'] or 0
        pct_otm = record['pct_otm'] or 0

        # Score the urgency
        score = 0
        reasons = []

        # Vol/OI ratio scoring
        if vol_oi >= THRESHOLDS['vol_oi_ratio_critical']:
            score += 40
            reasons.append(f"Vol/OI {vol_oi:.0f}x (EXTREME)")
        elif vol_oi >= THRESHOLDS['vol_oi_ratio_high']:
            score += 25
            reasons.append(f"Vol/OI {vol_oi:.0f}x (very high)")
        elif vol_oi >= THRESHOLDS['vol_oi_ratio_min']:
            score += 10
            reasons.append(f"Vol/OI {vol_oi:.1f}x")

        # Premium scoring
        if premium >= THRESHOLDS['premium_critical']:
            score += 35
            reasons.append(f"${premium:,.0f} premium (WHALE)")
        elif premium >= THRESHOLDS['premium_high']:
            score += 20
            reasons.append(f"${premium:,.0f} premium (big bet)")
        elif premium >= THRESHOLDS['premium_min']:
            score += 8
            reasons.append(f"${premium:,.0f} premium")

        # Speculative OTM scoring
        if pct_otm > THRESHOLDS['otm_pct_speculative'] and record['dte'] and record['dte'] < 14:
            score += 20
            reasons.append(f"{pct_otm:.0f}% OTM + {record['dte']}d to expiry (SPECULATIVE)")
        elif pct_otm > THRESHOLDS['otm_pct_speculative']:
            score += 10
            reasons.append(f"{pct_otm:.0f}% OTM")

        # Near-term expiry bonus
        if record['dte'] and record['dte'] <= 7:
            score += 10
            reasons.append(f"Expires in {record['dte']}d")

        # Determine severity
        if score >= 60:
            severity = 'CRITICAL'
        elif score >= 35:
            severity = 'HIGH'
        elif score >= 18:
            severity = 'MEDIUM'
        else:
            return None  # Not worth alerting

        # Determine alert type
        if premium >= THRESHOLDS['premium_critical']:
            alert_type = 'SMART_MONEY'
        elif vol_oi >= THRESHOLDS['vol_oi_ratio_high']:
            alert_type = 'VOL_SPIKE'
        elif premium >= THRESHOLDS['premium_high']:
            alert_type = 'BIG_PREMIUM'
        elif record['contract_type'] == 'PUT' and vol_oi >= THRESHOLDS['vol_oi_ratio_min']:
            alert_type = 'PUT_WALL'
        else:
            alert_type = 'CALL_SWEEP'

        ticker = record['ticker']
        ct = record['contract_type']
        strike = record['strike']
        exp = record['expiration']
        description = (
            f"{'▼' if ct == 'PUT' else '▲'} {ticker} {ct} ${strike:.0f} "
            f"exp {exp} | {' + '.join(reasons)} | "
            f"Sentiment: {record['sentiment']}"
        )

        return {
            'alert_date': self.scan_date,
            'alert_time': self.scan_time,
            'ticker': ticker,
            'alert_type': alert_type,
            'severity': severity,
            'contract_type': ct,
            'strike': strike,
            'expiration': exp,
            'vol_oi_ratio': vol_oi,
            'premium_total': premium,
            'underlying_price': record['underlying_price'],
            'description': description,
            'chekov_match': 1,  # if we're scanning it, it's on our list
            'convergence_score': min(score, 100),
        }

    # ------------------------------------------------------------------
    # Database operations
    # ------------------------------------------------------------------

    def _store_flow(self, records: list[dict]):
        if not records:
            return
        conn = sqlite3.connect(self.db_path)
        for r in records:
            try:
                conn.execute("""
                    INSERT OR IGNORE INTO uoa_flow
                    (scan_date, scan_time, ticker, contract_type, strike,
                     expiration, dte, volume, open_interest, vol_oi_ratio,
                     last_price, bid, ask, implied_volatility, premium_total,
                     moneyness, underlying_price, pct_otm, sentiment, source)
                    VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
                """, (
                    r['scan_date'], r['scan_time'], r['ticker'],
                    r['contract_type'], r['strike'], r['expiration'],
                    r['dte'], r['volume'], r['open_interest'],
                    r['vol_oi_ratio'], r['last_price'], r['bid'], r['ask'],
                    r['implied_volatility'], r['premium_total'],
                    r['moneyness'], r['underlying_price'], r['pct_otm'],
                    r['sentiment'], r['source'],
                ))
            except sqlite3.Error:
                continue
        conn.commit()
        conn.close()

    def _store_alerts(self, alerts: list[dict]):
        if not alerts:
            return
        conn = sqlite3.connect(self.db_path)
        for a in alerts:
            try:
                conn.execute("""
                    INSERT INTO uoa_alerts
                    (alert_date, alert_time, ticker, alert_type, severity,
                     contract_type, strike, expiration, vol_oi_ratio,
                     premium_total, underlying_price, description,
                     chekov_match, convergence_score)
                    VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?)
                """, (
                    a['alert_date'], a['alert_time'], a['ticker'],
                    a['alert_type'], a['severity'], a['contract_type'],
                    a['strike'], a['expiration'], a['vol_oi_ratio'],
                    a['premium_total'], a['underlying_price'],
                    a['description'], a.get('chekov_match', 0),
                    a.get('convergence_score', 0),
                ))
            except sqlite3.Error:
                continue
        conn.commit()
        conn.close()

    def _store_summaries(self, summaries: list[dict]):
        if not summaries:
            return
        conn = sqlite3.connect(self.db_path)
        for s in summaries:
            try:
                conn.execute("""
                    INSERT OR REPLACE INTO uoa_daily_summary
                    (scan_date, ticker, total_call_volume, total_put_volume,
                     put_call_ratio, total_call_premium, total_put_premium,
                     premium_put_call_ratio, max_vol_oi_ratio,
                     unusual_contracts, avg_iv, underlying_close)
                    VALUES (?,?,?,?,?,?,?,?,?,?,?,?)
                """, (
                    s['scan_date'], s['ticker'], s['total_call_volume'],
                    s['total_put_volume'], s['put_call_ratio'],
                    s['total_call_premium'], s['total_put_premium'],
                    s['premium_put_call_ratio'], s['max_vol_oi_ratio'],
                    s['unusual_contracts'], s['avg_iv'],
                    s['underlying_close'],
                ))
            except sqlite3.Error:
                continue
        conn.commit()
        conn.close()

    def _log_scan(self, scan_type, tickers, contracts, alerts, errors, duration):
        conn = sqlite3.connect(self.db_path)
        conn.execute("""
            INSERT INTO uoa_scan_log
            (scan_date, scan_time, scan_type, tickers_scanned,
             contracts_found, alerts_generated, errors, duration_seconds)
            VALUES (?,?,?,?,?,?,?,?)
        """, (
            self.scan_date, self.scan_time, scan_type, tickers,
            contracts, alerts,
            '; '.join(errors[:10]) if errors else None,
            round(duration, 1),
        ))
        conn.commit()
        conn.close()

    # ------------------------------------------------------------------
    # Watchlist helpers
    # ------------------------------------------------------------------

    def _get_chekov_watchlist(self) -> list[str]:
        """Pull Chekov's 528-stock watchlist from trader.db."""
        try:
            conn = sqlite3.connect(self.db_path)
            # Try the scanner_universe table first
            rows = conn.execute(
                "SELECT DISTINCT ticker FROM scanner_universe "
                "ORDER BY ticker"
            ).fetchall()
            if not rows:
                # Fallback: try universe_scan
                rows = conn.execute(
                    "SELECT DISTINCT ticker FROM universe_scan "
                    "ORDER BY ticker"
                ).fetchall()
            conn.close()
            return [r[0] for r in rows] if rows else []
        except sqlite3.Error:
            return []

    def _get_top_universe(self, top_n: int = 50) -> list[str]:
        """Get top N stocks from Chekov's latest universe scan by score."""
        try:
            conn = sqlite3.connect(self.db_path)
            rows = conn.execute("""
                SELECT ticker FROM universe_scan
                WHERE scan_date = (SELECT MAX(scan_date) FROM universe_scan)
                ORDER BY score DESC
                LIMIT ?
            """, (top_n,)).fetchall()
            conn.close()
            return [r[0] for r in rows] if rows else []
        except sqlite3.Error:
            return []

    def _get_default_watchlist(self) -> list[str]:
        """Fallback watchlist: mega-caps + popular options tickers."""
        return [
            # Mag 7 + major tech
            'AAPL', 'MSFT', 'GOOGL', 'AMZN', 'META', 'NVDA', 'TSLA',
            # Semiconductors
            'AMD', 'MU', 'AVGO', 'QCOM', 'INTC', 'ARM', 'MRVL',
            # Cloud / Software
            'CRM', 'NOW', 'PLTR', 'SNOW', 'NET', 'DDOG',
            # Financials
            'JPM', 'GS', 'BAC', 'MS', 'V', 'MA',
            # Healthcare
            'UNH', 'LLY', 'JNJ', 'PFE', 'MRNA', 'ABBV',
            # Energy
            'XOM', 'CVX', 'OXY', 'XLE', 'SLB',
            # Consumer
            'COST', 'WMT', 'TGT', 'NKE', 'DIS', 'NFLX',
            # Industrial / AI infra
            'DELL', 'VRT', 'SMCI',
            # Meme / High-vol
            'GME', 'AMC', 'RIVN', 'COIN', 'MARA',
            # ETFs (heavy options flow)
            'SPY', 'QQQ', 'IWM', 'XLF', 'XLE', 'GLD', 'SLV',
        ]


# ---------------------------------------------------------------------------
# Standalone runner
# ---------------------------------------------------------------------------
if __name__ == '__main__':
    import sys

    db = 'trader.db'
    mode = sys.argv[1] if len(sys.argv) > 1 else 'quick'

    scraper = UOAScraper(db_path=db)

    if mode == 'full':
        results = scraper.scan_watchlist()
    elif mode == 'quick':
        results = scraper.scan_quick(top_n=50)
    else:
        # Treat as comma-separated tickers
        tickers = mode.upper().split(',')
        results = scraper.scan_tickers(tickers)

    # Print critical and high alerts
    print("\n" + "=" * 70)
    print("  UNUSUAL OPTIONS ACTIVITY ALERTS")
    print("=" * 70)

    for alert in results.get('alerts', []):
        if alert['severity'] in ('CRITICAL', 'HIGH'):
            icon = '🚨' if alert['severity'] == 'CRITICAL' else '⚠️'
            print(f"\n{icon} [{alert['severity']}] {alert['description']}")

    if not any(a['severity'] in ('CRITICAL', 'HIGH')
               for a in results.get('alerts', [])):
        print("\nNo critical or high alerts detected.")

    print(f"\nScan summary: {results['tickers_scanned']} tickers, "
          f"{results['alerts_generated']} alerts, "
          f"{results['duration_seconds']}s")
