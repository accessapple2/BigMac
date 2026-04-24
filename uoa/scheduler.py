"""
TradeMinds UOA Scheduler
========================
Runs UOA scans on a schedule. Can be called from:
  - launchd (macOS) alongside existing pre-market service
  - cron
  - The existing main.py scheduler loop
  - Manually: python -m uoa.scheduler

Schedule (Arizona MST = UTC-7, no DST):
  - 4:30 PM MST (market close + 30min): FULL scan of Chekov's 528 stocks
    This catches end-of-day positioning before after-hours.
  - 6:00 AM MST (pre-market): QUICK scan of top 50
    This catches overnight/pre-market flow changes.
  - On-demand via API: /api/uoa/scan/tickers?tickers=META,NVDA
"""

import sys
import os
import time
from datetime import datetime, timedelta

# Add parent dir to path so we can import from project root
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from uoa.scraper import UOAScraper


def get_mst_now():
    """Get current time in MST (UTC-7, no DST for Arizona)."""
    return datetime.utcnow() - timedelta(hours=7)


def should_run_post_close_scan():
    """Run at 4:30 PM MST on weekdays (30 min after market close)."""
    now = get_mst_now()
    return (now.hour == 16 and 25 <= now.minute <= 35 and
            now.weekday() < 5)


def should_run_premarket_scan():
    """Run at 6:00 AM MST on weekdays (with existing pre-market service)."""
    now = get_mst_now()
    return (now.hour == 6 and now.minute < 10 and
            now.weekday() < 5)


def run_post_close():
    """Full 528-stock scan after market close."""
    print(f"\n{'='*60}")
    print(f"  UOA POST-CLOSE FULL SCAN - {get_mst_now().strftime('%Y-%m-%d %H:%M MST')}")
    print(f"{'='*60}")
    scraper = UOAScraper()
    return scraper.scan_watchlist()


def run_premarket():
    """Quick top-50 scan before market open."""
    print(f"\n{'='*60}")
    print(f"  UOA PRE-MARKET QUICK SCAN - {get_mst_now().strftime('%Y-%m-%d %H:%M MST')}")
    print(f"{'='*60}")
    scraper = UOAScraper()
    return scraper.scan_quick(top_n=50)


def scheduler_loop():
    """
    Simple scheduler loop. Run this if you want standalone scheduling.
    For most setups, integrate with existing main.py scheduler instead.
    """
    print("[UOA Scheduler] Starting... (Ctrl+C to stop)")
    last_post_close = None
    last_premarket = None

    while True:
        today = get_mst_now().strftime('%Y-%m-%d')

        if should_run_post_close_scan() and last_post_close != today:
            run_post_close()
            last_post_close = today

        if should_run_premarket_scan() and last_premarket != today:
            run_premarket()
            last_premarket = today

        time.sleep(60)  # Check every minute


if __name__ == '__main__':
    if len(sys.argv) > 1:
        mode = sys.argv[1]
        if mode == 'post-close':
            run_post_close()
        elif mode == 'premarket':
            run_premarket()
        elif mode == 'loop':
            scheduler_loop()
        else:
            print(f"Usage: python -m uoa.scheduler [post-close|premarket|loop]")
    else:
        # Default: run a quick scan right now
        scraper = UOAScraper()
        results = scraper.scan_quick(top_n=50)
