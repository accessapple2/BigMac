#!/usr/bin/env python3
"""Generate a crew advisory report from the latest Schwab holdings snapshot.

Usage:
    python3 scripts/schwab_advisor.py
    python3 scripts/schwab_advisor.py --snapshot-id 2026-04-24T12:48:00
    python3 scripts/schwab_advisor.py --output ~/Desktop/advisory.md
    python3 scripts/schwab_advisor.py --stdout   # print only, no file write

Output defaults to ~/autonomous-trader/schwab_advisory_YYYYMMDD.md
"""
from __future__ import annotations

import os
import sqlite3
import sys
from datetime import datetime, timezone, timedelta
from pathlib import Path

REPO_ROOT   = Path(os.environ.get("BIGMAC_REPO", "/Users/bigmac/autonomous-trader"))
DB_PATH     = REPO_ROOT / "data" / "trader.db"
SIGNALS_DB  = REPO_ROOT / "signal-center" / "signals.db"

SECTOR_MAP: dict[str, str] = {
    "AMD":  "Semiconductors",
    "AMZN": "E-commerce/Cloud",
    "AVGO": "Semiconductors",
    "CRWD": "Cybersecurity",
    "DELL": "Hardware",
    "PLTR": "Data/AI",
    "VRT":  "Infrastructure",
}

# ─── Data loaders ─────────────────────────────────────────────────────────────

def _latest_snapshot_id(conn: sqlite3.Connection) -> str | None:
    row = conn.execute(
        "SELECT snapshot_id FROM schwab_holdings ORDER BY snapshot_id DESC LIMIT 1"
    ).fetchone()
    return row[0] if row else None


def _load_snapshot(conn: sqlite3.Connection, snapshot_id: str) -> list[dict]:
    cur = conn.execute(
        """
        SELECT symbol, description, qty, price, market_value, cost_basis,
               gain_dollar, gain_pct, day_change_dollar, day_change_pct,
               asset_type, snapshot_ts, account_label, account_last4
        FROM schwab_holdings
        WHERE snapshot_id = ? AND is_summary_row = 0
        ORDER BY CASE symbol WHEN 'CASH' THEN 'ZZZ' ELSE symbol END
        """,
        (snapshot_id,)
    )
    cols = [d[0] for d in cur.description]
    return [dict(zip(cols, row)) for row in cur.fetchall()]


def _load_signals(symbols: list[str]) -> list[dict]:
    """Query both signal DBs for the given symbols, last 7 days."""
    cutoff = (datetime.now(timezone.utc) - timedelta(days=7)).strftime("%Y-%m-%d")
    placeholders = ",".join("?" * len(symbols))
    results: list[dict] = []

    # Source 1: trader.db → signals (player_id, symbol, signal, confidence, created_at)
    try:
        conn = sqlite3.connect(str(DB_PATH))
        rows = conn.execute(
            f"""
            SELECT player_id AS agent, symbol, signal AS action,
                   confidence, created_at, 'player' AS source
            FROM signals
            WHERE symbol IN ({placeholders})
              AND created_at >= ?
            ORDER BY created_at DESC
            LIMIT 50
            """,
            (*symbols, cutoff)
        ).fetchall()
        conn.close()
        for r in rows:
            results.append(dict(zip(
                ["agent", "symbol", "action", "confidence", "created_at", "source"], r
            )))
    except Exception as e:
        results.append({"_error": f"trader.db signals query failed: {e}"})

    # Source 2: signal-center/signals.db → trade_signals
    if SIGNALS_DB.exists():
        try:
            conn2 = sqlite3.connect(str(SIGNALS_DB))
            rows2 = conn2.execute(
                f"""
                SELECT agent_name AS agent, symbol, action,
                       confidence, created_at, 'agent' AS source
                FROM trade_signals
                WHERE symbol IN ({placeholders})
                  AND created_at >= ?
                ORDER BY created_at DESC
                LIMIT 20
                """,
                (*symbols, cutoff)
            ).fetchall()
            conn2.close()
            for r in rows2:
                results.append(dict(zip(
                    ["agent", "symbol", "action", "confidence", "created_at", "source"], r
                )))
        except Exception as e:
            results.append({"_error": f"signal-center signals query failed: {e}"})

    return results

# ─── Formatting helpers ───────────────────────────────────────────────────────

def _fmt_dollars(v: float | None, plus: bool = False) -> str:
    if v is None:
        return "N/A"
    sign = "+" if plus and v > 0 else ""
    return f"{sign}${v:,.2f}"

def _fmt_pct(v: float | None, plus: bool = False) -> str:
    if v is None:
        return "N/A"
    sign = "+" if plus and v > 0 else ""
    return f"{sign}{v:.2f}%"

def _gain_flag(gain_dollar: float | None) -> str:
    if gain_dollar is None:
        return ""
    return "🟢" if gain_dollar >= 0 else "🔴"

# ─── Advisory generator ───────────────────────────────────────────────────────

def generate_advisory(snapshot_id: str | None = None) -> str:
    conn = sqlite3.connect(str(DB_PATH))
    try:
        sid = snapshot_id or _latest_snapshot_id(conn)
        if sid is None:
            return "ERROR: no schwab_holdings snapshots found in trader.db"

        rows = _load_snapshot(conn, sid)
    finally:
        conn.close()

    if not rows:
        return f"ERROR: no rows found for snapshot_id={sid!r}"

    meta        = rows[0]
    now_str     = datetime.now().strftime("%Y-%m-%d %H:%M AZ")
    equity_rows = [r for r in rows if r["asset_type"] == "Equity"]
    cash_row    = next((r for r in rows if r["symbol"] == "CASH"), None)

    total_value     = sum(r["market_value"] for r in rows if r["market_value"])
    equity_value    = sum(r["market_value"] for r in equity_rows if r["market_value"])
    cash_value      = cash_row["market_value"] if cash_row else 0.0
    total_gain      = sum(r["gain_dollar"] for r in equity_rows if r["gain_dollar"] is not None)
    total_day_chg   = sum(r["day_change_dollar"] for r in rows if r["day_change_dollar"] is not None)

    cash_pct        = (cash_value  / total_value * 100) if total_value else 0
    equity_pct      = (equity_value / total_value * 100) if total_value else 0

    equity_symbols = [r["symbol"] for r in equity_rows]
    all_signals    = _load_signals(equity_symbols)
    signal_errors  = [s["_error"] for s in all_signals if "_error" in s]
    clean_signals  = [s for s in all_signals if "_error" not in s]

    lines: list[str] = []

    # ── §1 Header ─────────────────────────────────────────────────────────────
    lines += [
        f"# Schwab Portfolio Advisory — {sid}",
        f"",
        f"**Account:** {meta['account_label']} (...{meta['account_last4']})  ",
        f"**Snapshot:** {meta['snapshot_ts']}  ",
        f"**Generated:** {now_str}",
        f"",
    ]

    # ── Callout: How to read this ──────────────────────────────────────────────
    lines += [
        "> **Read this as:** a point-in-time snapshot of your Schwab positions with context",
        "> from Ollie's recent signal history. It's data, not recommendations. Agent-driven",
        "> advisory requires the Schwab API path (submitted, 48hr approval pending).",
        f"",
    ]

    # ── §2 Portfolio Summary ───────────────────────────────────────────────────
    lines += [
        "## Portfolio Summary",
        f"",
        f"| | |",
        f"|---|---|",
        f"| **Total Market Value** | {_fmt_dollars(total_value)} |",
        f"| **Equity** | {_fmt_dollars(equity_value)} ({equity_pct:.1f}% of portfolio) |",
        f"| **Cash** | {_fmt_dollars(cash_value)} ({cash_pct:.1f}% of portfolio) |",
        f"| **Total Unrealized Gain/Loss** | {_fmt_dollars(total_gain, plus=True)} |",
        f"| **Today's Change** | {_fmt_dollars(total_day_chg, plus=True)} |",
        f"",
    ]

    # ── §3 Holdings Table ──────────────────────────────────────────────────────
    lines += [
        "## Holdings",
        f"",
        f"| Symbol | Qty | Price | Mkt Value | % Portfolio | Gain $ | Gain % | Day Chg % |",
        f"|--------|----:|------:|----------:|------------:|-------:|-------:|----------:|",
    ]
    for r in sorted(equity_rows, key=lambda x: x["market_value"] or 0, reverse=True):
        flag   = _gain_flag(r["gain_dollar"])
        pct_of = (r["market_value"] / total_value * 100) if total_value and r["market_value"] else 0
        lines.append(
            f"| {flag} {r['symbol']:<5} "
            f"| {int(r['qty']) if r['qty'] and r['qty'] == int(r['qty']) else r['qty']} "
            f"| {_fmt_dollars(r['price'])} "
            f"| {_fmt_dollars(r['market_value'])} "
            f"| {pct_of:.1f}% "
            f"| {_fmt_dollars(r['gain_dollar'], plus=True)} "
            f"| {_fmt_pct(r['gain_pct'], plus=True)} "
            f"| {_fmt_pct(r['day_change_pct'], plus=True)} |"
        )
    lines.append(f"")

    # ── §4 Attention Flags ─────────────────────────────────────────────────────
    lines += ["## Attention Flags", f""]
    flags_found: list[str] = []
    for r in equity_rows:
        sym   = r["symbol"]
        dcp   = r["day_change_pct"]
        gp    = r["gain_pct"]
        mv    = r["market_value"] or 0
        conc  = (mv / total_value * 100) if total_value else 0

        if dcp is not None and abs(dcp) > 10:
            flags_found.append(f"- **{sym}**: ⚠️ Large intraday move ({_fmt_pct(dcp, plus=True)})")
        if dcp is not None and dcp >= 14.5:
            flags_found.append(f"- **{sym}**: ⚠️ Approaching 15% circuit threshold ({_fmt_pct(dcp, plus=True)})")
        if gp is not None and gp < -10:
            flags_found.append(f"- **{sym}**: 🔴 Significant loss position ({_fmt_pct(gp, plus=True)})")
        if gp is not None and gp > 20:
            flags_found.append(f"- **{sym}**: 🟢 Significant winner ({_fmt_pct(gp, plus=True)})")
        if conc > 15:
            flags_found.append(f"- **{sym}**: ⚠️ Concentration ({conc:.1f}% of total portfolio)")

    if flags_found:
        lines += flags_found
    else:
        lines.append("No symbols triggered attention flags in this snapshot.")
    lines.append(f"")

    # ── §5 Sector/Concentration ────────────────────────────────────────────────
    lines += ["## Sector / Concentration", f""]
    sector_totals: dict[str, float] = {}
    for r in equity_rows:
        sector = SECTOR_MAP.get(r["symbol"], "Other")
        sector_totals[sector] = sector_totals.get(sector, 0) + (r["market_value"] or 0)

    lines += [
        "| Sector | Equity Allocation | % of Equity |",
        "|--------|------------------:|------------:|",
    ]
    sector_flags: list[str] = []
    for sector, val in sorted(sector_totals.items(), key=lambda x: x[1], reverse=True):
        pct = (val / equity_value * 100) if equity_value else 0
        lines.append(f"| {sector} | {_fmt_dollars(val)} | {pct:.1f}% |")
        if pct >= 40:
            sector_flags.append(f"- ⚠️ **{sector}** at {pct:.1f}% of equity — sector concentration")
    lines.append(f"")
    if sector_flags:
        lines += sector_flags
        lines.append(f"")

    # ── §6 Signal Cross-reference ──────────────────────────────────────────────
    lines += ["## Ollie Signal Cross-reference (Last 7 Days)", f""]

    if signal_errors:
        for err in signal_errors:
            lines.append(f"> ⚠️ {err}")
        lines.append(f"")

    # Group by symbol, keep most recent per symbol per source
    by_symbol: dict[str, list[dict]] = {s: [] for s in equity_symbols}
    for sig in clean_signals:
        sym = sig.get("symbol", "")
        if sym in by_symbol:
            by_symbol[sym].append(sig)

    no_signal_syms: list[str] = []
    for sym in sorted(equity_symbols):
        sig_list = by_symbol[sym]
        if not sig_list:
            no_signal_syms.append(sym)
            continue
        # Most recent first (already sorted DESC from query)
        lines.append(f"**{sym}**")
        seen = 0
        for sig in sig_list[:3]:
            action     = (sig.get("action") or "").upper()
            agent      = sig.get("agent") or "unknown"
            conf       = sig.get("confidence")
            conf_str   = f"{conf:.0f}%" if conf is not None else "N/A"
            ts         = (sig.get("created_at") or "")[:16]
            source_tag = f"[{sig.get('source', '?')}]"
            lines.append(f"- `{ts}` {action} conf={conf_str} — {agent} {source_tag}")
            seen += 1
        if len(sig_list) > 3:
            lines.append(f"- _{len(sig_list) - 3} more signal(s) in window_")
        lines.append(f"")

    if no_signal_syms:
        lines.append(f"**No recent signals (7d):** {', '.join(no_signal_syms)}")
        lines.append(f"")

    # ── §7 Dry Powder ─────────────────────────────────────────────────────────
    lines += ["## Dry Powder Analysis", f""]
    avg_equity_position = equity_value / len(equity_rows) if equity_rows else 0
    lines += [
        f"- **Cash available:** {_fmt_dollars(cash_value)} ({cash_pct:.1f}% of portfolio)",
        f"- **Equity deployed:** {_fmt_dollars(equity_value)} across {len(equity_rows)} positions",
        f"- **Avg position size:** {_fmt_dollars(avg_equity_position)}",
        f"",
    ]
    if cash_pct > 50:
        lines.append(
            f"⚡ **High cash allocation ({cash_pct:.1f}%)** — significant dry powder available "
            f"for deployment. At current avg position size ({_fmt_dollars(avg_equity_position)}), "
            f"cash covers ~{cash_value / avg_equity_position:.0f} additional positions."
        )
    elif cash_pct < 5:
        lines.append("⚠️ **Low cash cushion (<5%)** — limited reserve for new entries or drawdown buffer.")
    else:
        lines.append(f"Cash allocation is within normal range.")
    lines.append(f"")

    # ── §8 Footer ──────────────────────────────────────────────────────────────
    lines += [
        "---",
        f"",
        "_Advisory only. No trades placed. Review before acting._  ",
        f"_Re-run: `python3 scripts/schwab_advisor.py`_  ",
        f"_Source snapshot: `snapshot_id={sid}`_",
    ]

    return "\n".join(lines)

# ─── Entry point ──────────────────────────────────────────────────────────────

def main() -> None:
    import argparse
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--snapshot-id", default=None, help="Override snapshot to use")
    parser.add_argument("--output", default=None, help="Output markdown path")
    parser.add_argument("--stdout", action="store_true", help="Print to stdout only, no file write")
    args = parser.parse_args()

    md = generate_advisory(snapshot_id=args.snapshot_id)

    print(md)

    if not args.stdout:
        date_str  = datetime.now().strftime("%Y%m%d")
        out_path  = Path(args.output) if args.output else REPO_ROOT / f"schwab_advisory_{date_str}.md"
        out_path.write_text(md, encoding="utf-8")
        print(f"\n[saved] {out_path}", file=sys.stderr)


if __name__ == "__main__":
    main()
