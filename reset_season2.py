"""
Season 2 Reset Script for TradeMinds Arena.

- Resets all AI player cash ($10,000 standard, $5,000 DayBlade)
- Clears all open positions
- Preserves ALL historical data (trades, signals, journal, etc.)
- Adds season column, marks existing data as season=1
- Sets all players to season=2
"""

import sqlite3

DB_PATH = "data/trader.db"


def reset():
    conn = sqlite3.connect(DB_PATH)
    conn.execute("PRAGMA journal_mode=WAL")
    c = conn.cursor()

    # --- Step 1: Add season columns (migration) ---
    migrations = [
        ("ai_players", "season", "INTEGER DEFAULT 1"),
        ("trades", "season", "INTEGER DEFAULT 1"),
        ("signals", "season", "INTEGER DEFAULT 1"),
        ("portfolio_history", "season", "INTEGER DEFAULT 1"),
    ]
    for table, col, typedef in migrations:
        try:
            c.execute(f"ALTER TABLE {table} ADD COLUMN {col} {typedef}")
            print(f"  Added {col} column to {table}")
        except sqlite3.OperationalError:
            print(f"  {col} column already exists on {table}")

    # --- Step 2: Mark ALL existing data as Season 1 ---
    c.execute("UPDATE trades SET season = 1 WHERE season IS NULL")
    c.execute("UPDATE signals SET season = 1 WHERE season IS NULL")
    c.execute("UPDATE portfolio_history SET season = 1 WHERE season IS NULL")
    c.execute("UPDATE ai_players SET season = 1 WHERE season IS NULL")
    print(f"  Marked existing trades as season 1: {c.rowcount} rows")

    # --- Step 3: Clear all open positions ---
    c.execute("SELECT COUNT(*) FROM positions")
    pos_count = c.fetchone()[0]
    c.execute("DELETE FROM positions")
    print(f"  Cleared {pos_count} open positions")

    # --- Step 4: Reset cash and set season=2 ---
    c.execute("UPDATE ai_players SET cash = 10000.00, season = 2, is_halted = 0, halt_reason = NULL WHERE id != 'dayblade-0dte'")
    c.execute("UPDATE ai_players SET cash = 5000.00, season = 2, is_halted = 0, halt_reason = NULL WHERE id = 'dayblade-0dte'")
    print("  Reset all player cash: $10,000 (DayBlade: $5,000)")
    print("  Set all players to Season 2")
    print("  Cleared all halt flags")

    # --- Step 5: Store season metadata in settings ---
    c.execute("INSERT OR REPLACE INTO settings (key, value) VALUES ('current_season', '2')")
    c.execute("INSERT OR REPLACE INTO settings (key, value) VALUES ('season_2_start', datetime('now'))")
    print("  Saved season metadata to settings table")

    conn.commit()

    # --- Verify ---
    print("\n--- Verification ---")
    for row in c.execute("SELECT id, display_name, cash, season, is_halted FROM ai_players ORDER BY id").fetchall():
        print(f"  {row[0]:20s} | ${row[2]:>10.2f} | Season {row[3]} | Halted: {row[4]}")
    print(f"  Open positions: {c.execute('SELECT COUNT(*) FROM positions').fetchone()[0]}")
    print(f"  Season 1 trades preserved: {c.execute('SELECT COUNT(*) FROM trades WHERE season=1').fetchone()[0]}")
    print(f"  Season 1 signals preserved: {c.execute('SELECT COUNT(*) FROM signals WHERE season=1').fetchone()[0]}")

    conn.close()
    print("\nSeason 2 reset complete! Restart the scanner to begin trading.")


if __name__ == "__main__":
    reset()
