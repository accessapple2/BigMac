#!/usr/bin/env python3
"""Situation Report — fleet status snapshot.

Runs on schedule (06:30 / 10:00 / 13:30 AZ) via launchd.
Outputs full report to logs/sitrep_<timestamp>.txt and pushes a
3-line summary via ntfy to ollietrades-admin.

Manual run:
    cd ~/autonomous-trader && python3 scripts/situation_report.py
"""
from __future__ import annotations
import sys, os, subprocess, sqlite3
from datetime import datetime
from pathlib import Path

ROOT = Path("/Users/bigmac/autonomous-trader")
sys.path.insert(0, str(ROOT))
os.chdir(ROOT)

LOG_PATH   = ROOT / "logs" / "trader.log"
SITREP_DIR = ROOT / "logs"
SITREP_DIR.mkdir(exist_ok=True)


def section(title: str) -> str:
    return f"\n=== {title} ===\n"


def safe_run(cmd: list, timeout: int = 5) -> str:
    try:
        return subprocess.check_output(cmd, text=True, timeout=timeout,
                                       stderr=subprocess.DEVNULL).strip()
    except Exception:
        return ""


def gather() -> tuple[str, dict]:
    """Build the full report text and a summary dict for ntfy."""
    summary = {
        "process_alive": False,
        "pid": None,
        "equity": None,
        "positions": None,
        "errors_24h": 0,
        "ped_signals_24h": 0,
        "bbd_signals_24h": 0,
        "spy": None,
        "vix": None,
        "regime": None,
    }
    parts = []
    now = datetime.now()
    parts.append("=" * 70)
    parts.append(f"  SITREP — {now.strftime('%Y-%m-%d %H:%M:%S AZ')}")
    parts.append("=" * 70)

    # Process check
    parts.append(section("Process"))
    out = safe_run(["pgrep", "-af", "main.py"])
    if out:
        summary["process_alive"] = True
        try:
            summary["pid"] = int(out.split()[0])
        except Exception:
            pass
        parts.append(f"  main.py: {out}")
    else:
        parts.append("  main.py: NOT RUNNING")

    # Last 20 trader log lines
    parts.append(section("Trader log — last 20 lines"))
    if LOG_PATH.exists():
        out = safe_run(["tail", "-20", str(LOG_PATH)])
        parts.append(out or "  [empty]")

    # Signals last 24h
    parts.append(section("PED / BBD signals (last 24h)"))
    if LOG_PATH.exists():
        ped = safe_run(["grep", "-cE", "PED SHORT|post_earnings_drift", str(LOG_PATH)])
        bbd = safe_run(["grep", "-cE", "BEAR BREAKDOWN|bear_momentum_breakdown", str(LOG_PATH)])
        try:
            summary["ped_signals_24h"] = int(ped or 0)
            summary["bbd_signals_24h"] = int(bbd or 0)
        except Exception:
            pass
        parts.append(f"  PED signals: {summary['ped_signals_24h']}")
        parts.append(f"  BBD signals: {summary['bbd_signals_24h']}")

    # Errors last 24h
    parts.append(section("Errors / red lines"))
    if LOG_PATH.exists():
        n = safe_run(["grep", "-cE", "ERROR|Traceback|\\[red\\]", str(LOG_PATH)])
        try:
            summary["errors_24h"] = int(n or 0)
        except Exception:
            pass
        parts.append(f"  red/error count: {summary['errors_24h']}")
        last_errs = safe_run(["grep", "-E", "Traceback|ERROR", str(LOG_PATH)])
        if last_errs:
            tail = last_errs.split("\n")[-3:]
            parts.append("  last 3 errors:")
            for ln in tail:
                parts.append(f"    {ln[:140]}")

    # Portfolio sync
    parts.append(section("Portfolio"))
    if LOG_PATH.exists():
        sync = safe_run(["grep", "-E", "\\[SYNC\\] Portfolio:", str(LOG_PATH)])
        if sync:
            last = sync.split("\n")[-1]
            parts.append(f"  {last}")
            try:
                if "Portfolio: $" in last:
                    eq = last.split("Portfolio: $")[1].split(" |")[0].replace(",", "")
                    summary["equity"] = float(eq)
                if "positions" in last:
                    pos = last.split("|")[2].strip().split()[0]
                    summary["positions"] = int(pos)
            except Exception:
                pass

    # Strategies registry
    parts.append(section("Strategies"))
    try:
        from engine.strategies import STRATEGIES
        parts.append(f"  total: {len(STRATEGIES)}")
        parts.append(f"  bull_momentum_breakout : {'bull_momentum_breakout' in STRATEGIES}")
        parts.append(f"  bear_momentum_breakdown: {'bear_momentum_breakdown' in STRATEGIES}")
    except Exception as e:
        parts.append(f"  registry read error: {e}")

    # Market data
    parts.append(section("Market"))
    try:
        from engine.market_data import get_stock_price
        for sym, label in [("SPY", "SPY"), ("QQQ", "QQQ"), ("^VIX", "VIX")]:
            d = get_stock_price(sym)
            if d:
                p = d.get("price")
                cp = d.get("change_pct", 0)
                line = f"  {label:4s}: {p}  ({cp:+.2f}%)"
                parts.append(line)
                if sym == "SPY": summary["spy"] = p
                if sym == "^VIX": summary["vix"] = p
    except Exception as e:
        parts.append(f"  market read error: {e}")

    # Regime
    parts.append(section("Regime"))
    if LOG_PATH.exists():
        r = safe_run(["grep", "-E", "RedAlert.*\\| .*SPY",  str(LOG_PATH)])
        if r:
            last = r.split("\n")[-1]
            parts.append(f"  {last[:160]}")
            try:
                # "RedAlert ▶ GREEN (57) | CHOP SPY $693.23 | trend 32 | VIX 16.5"
                if "|" in last:
                    after_pipe = last.split("|")[1].strip()
                    summary["regime"] = after_pipe.split()[0]
            except Exception:
                pass

    # Earnings window
    parts.append(section("Earnings (next 7 days)"))
    if LOG_PATH.exists():
        e = safe_run(["grep", "-E", "Earnings next 7 days", str(LOG_PATH)])
        if e:
            parts.append(f"  {e.split(chr(10))[-1]}")

    parts.append("\n" + "=" * 70)
    return "\n".join(parts), summary


def push_ntfy(summary: dict) -> None:
    """3-line tactical summary to ollietrades-admin topic."""
    try:
        from engine.ntfy import _fire, P_DEFAULT
    except Exception as e:
        print(f"  ntfy import error: {e}")
        return

    proc = "✓" if summary["process_alive"] else "✗"
    eq = f"${summary['equity']:,.2f}" if summary['equity'] else "?"
    pos = f"{summary['positions']}p" if summary['positions'] is not None else "?p"
    err = summary["errors_24h"]
    ped = summary["ped_signals_24h"]
    bbd = summary["bbd_signals_24h"]
    spy = f"SPY {summary['spy']}" if summary['spy'] else ""
    vix = f"VIX {summary['vix']}" if summary['vix'] else ""
    regime = summary['regime'] or ""

    title = f"SITREP {proc} {eq} {pos}"
    body = (
        f"PED:{ped} BBD:{bbd} err:{err}\n"
        f"{spy}  {vix}  {regime}"
    ).strip()
    _fire(title, body, priority=P_DEFAULT, tags="clipboard")


def main():
    text, summary = gather()
    ts = datetime.now().strftime("%Y%m%d_%H%M%S")
    out_path = SITREP_DIR / f"sitrep_{ts}.txt"
    out_path.write_text(text)
    print(text)
    print(f"\n[saved] {out_path}")
    push_ntfy(summary)
    print("[ntfy] pushed")


if __name__ == "__main__":
    main()
