"""
OllieTrades Season 6 — Agent Deep Dive Report
Run: venv/bin/python3 -m engine.deep_dive_report
"""
import sqlite3

DB = "data/backtest.db"

def run():
    conn = sqlite3.connect(DB)
    print("\n" + "="*90)
    print("  OLLIETRADES SEASON 6 — AGENT DEEP DIVE REPORT")
    print("="*90)

    print("\n📊 ALL-TIME AGENT SUMMARY\n")
    rows = conn.execute("""
        SELECT agent_id,
               COUNT(*) as runs,
               ROUND(AVG(total_return),2) as avg_return,
               ROUND(MAX(total_return),2) as best_return,
               ROUND(AVG(win_rate),1) as avg_wr,
               ROUND(AVG(sharpe),3) as avg_sharpe,
               ROUND(AVG(expectancy),3) as avg_expectancy,
               ROUND(AVG(recovery_factor),3) as avg_recovery,
               SUM(num_trades) as total_trades
        FROM backtest_v5_sniper
        GROUP BY agent_id
        ORDER BY avg_return DESC
    """).fetchall()

    print(f"  {'Agent':<20} {'Runs':>4} {'AvgRet':>8} {'BestRet':>8} {'WR%':>6} {'Sharpe':>8} {'Expect':>8} {'Recovery':>9} {'Trades':>7}")
    print("  " + "-"*85)
    for r in rows:
        print(f"  {r[0]:<20} {r[1]:>4} {r[2]:>7}% {r[3]:>7}% {r[4]:>5}% {r[5]:>8} {r[6]:>8} {r[7]:>9} {r[8]:>7}")

    print("\n\n🧠 OLLIE GATE DECISIONS PER AGENT\n")
    rows = conn.execute("""
        SELECT agent_id,
               COUNT(*) as total,
               SUM(CASE WHEN decision='APPROVE' THEN 1 ELSE 0 END) as approved,
               SUM(CASE WHEN decision='REJECT' THEN 1 ELSE 0 END) as rejected,
               ROUND(AVG(ollie_score),3) as avg_score,
               ROUND(AVG(trade_alpha),3) as avg_alpha,
               ROUND(AVG(CASE WHEN decision='APPROVE' THEN ollie_score END),3) as appr_score,
               ROUND(AVG(CASE WHEN decision='REJECT' THEN ollie_score END),3) as rej_score
        FROM backtest_v5_ollie_decisions
        GROUP BY agent_id
        ORDER BY approved DESC
    """).fetchall()

    print(f"  {'Agent':<20} {'Total':>6} {'Appr':>6} {'Rej':>6} {'AvgScore':>9} {'AvgAlpha':>9} {'ApprScore':>10} {'RejScore':>9}")
    print("  " + "-"*80)
    for r in rows:
        print(f"  {str(r[0]):<20} {r[1]:>6} {r[2]:>6} {r[3]:>6} {str(r[4] or 0.0):>9} {str(r[5] or 0.0):>9} {str(r[6] or 0.0):>10} {str(r[7] or 0.0):>9}")

    print("\n\n🌡️  REGIME PERFORMANCE BY STRATEGY\n")
    rows = conn.execute("""
        SELECT strategy, regime,
               COUNT(*) as trades,
               ROUND(AVG(total_return),2) as avg_return,
               ROUND(AVG(win_rate),1) as avg_wr,
               ROUND(AVG(sharpe),3) as avg_sharpe
        FROM backtest_v5_sniper_master
        GROUP BY strategy, regime
        ORDER BY strategy, avg_return DESC
    """).fetchall()

    current = None
    for r in rows:
        if r[0] != current:
            current = r[0]
            print(f"\n  📈 {current}")
            print(f"  {'Regime':<12} {'Trades':>7} {'AvgRet':>8} {'WR%':>6} {'Sharpe':>8}")
            print("  " + "-"*45)
        print(f"  {(r[1] or 'UNKNOWN'):<12} {r[2]:>7} {r[3]:>7}% {r[4]:>5}% {r[5]:>8}")

    print("\n\n🎯 TOP TICKERS PER STRATEGY\n")
    rows = conn.execute("""
        SELECT strategy, ticker,
               COUNT(*) as trades,
               ROUND(SUM(total_return),2) as total_return,
               ROUND(AVG(win_rate),1) as avg_wr,
               ROUND(AVG(alpha_score),3) as avg_alpha
        FROM backtest_v5_sniper_master
        GROUP BY strategy, ticker
        ORDER BY strategy, total_return DESC
    """).fetchall()

    current = None
    for r in rows:
        if r[0] != current:
            current = r[0]
            print(f"\n  📊 {current}")
            print(f"  {'Strategy':<22} {'Trades':>7} {'TotRet':>8} {'WR%':>6} {'Alpha':>7}")
            print("  " + "-"*50)
        print(f"  {r[1]:<22} {r[2]:>7} {r[3]:>7}% {r[4]:>5}% {r[5]:>7}")

    print("\n\n👻 SHADOW TRADES — WHAT OLLIE BLOCKED\n")
    rows = conn.execute("""
        SELECT agent_id,
               COUNT(*) as blocked,
               SUM(CASE WHEN shadow_pnl_pct > 0 THEN 1 ELSE 0 END) as would_win,
               ROUND(AVG(shadow_pnl_pct),2) as avg_pnl,
               ROUND(AVG(ollie_score),3) as avg_score
        FROM backtest_v5_ollie_decisions
        WHERE decision='REJECT' AND shadow_pnl_pct IS NOT NULL
        GROUP BY agent_id
        ORDER BY blocked DESC
    """).fetchall()

    if rows:
        print(f"  {'Agent':<20} {'Blocked':>8} {'WouldWin':>9} {'AvgPnL':>8} {'AvgScore':>9}")
        print("  " + "-"*57)
        for r in rows:
            print(f"  {r[0]:<20} {r[1]:>8} {r[2]:>9} {r[3]:>7}% {r[4]:>9}")
    else:
        print("  No shadow trade data yet.")

    print("\n" + "="*90 + "\n")
    conn.close()

if __name__ == "__main__":
    run()
