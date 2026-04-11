#!/usr/bin/env bash
# ============================================================================
# USS TradeMinds — Launch Script
#
# Single server: main.py → port 8080 (all trading, scanning, dashboard)
# Port 8000 (main_crew.py) decommissioned 2026-04-07.
#
# Usage: ./launch-trademinds.sh [--crew | --scout | --servers | default: full dev mode]
# ============================================================================

set -euo pipefail

# ---------------------------------------------------------------------------
# Environment
# ---------------------------------------------------------------------------
export TRADEMINDS_DIR="${TRADEMINDS_DIR:-$HOME/autonomous-trader}"
export TRADEMINDS_DB="${TRADEMINDS_DB:-$TRADEMINDS_DIR/data/trader.db}"
VENV_ARENA="$TRADEMINDS_DIR/venv/bin/python3"

# Status symbols (colorblind-safe: symbols + text, not just colors)
OK="[OK]"
FAIL="[FAIL]"
WARN="[WARN]"
INFO="[INFO]"

# Track background PIDs for cleanup
ARENA_PID=""

cleanup() {
    echo ""
    echo "$INFO Shutting down..."
    [ -n "$ARENA_PID" ] && kill "$ARENA_PID" 2>/dev/null && echo "  Stopped Arena (PID $ARENA_PID)"
    exit 0
}
trap cleanup SIGINT SIGTERM

# ---------------------------------------------------------------------------
# Pre-flight checks
# ---------------------------------------------------------------------------
echo "========================================"
echo "  USS TradeMinds — Pre-flight Checks"
echo "========================================"
echo ""

# 0. Clean up stale Ollama lock (in case of prior crash)
OLLAMA_LOCK="/tmp/trademinds-ollama.lock"
if [ -f "$OLLAMA_LOCK" ]; then
    echo "$INFO Removing stale Ollama lock: $OLLAMA_LOCK"
    rm -f "$OLLAMA_LOCK"
fi

# 1. Directory exists
if [ -d "$TRADEMINDS_DIR" ]; then
    echo "$OK Directory: $TRADEMINDS_DIR"
else
    echo "$FAIL Directory not found: $TRADEMINDS_DIR"
    exit 1
fi

# 2. Database exists
if [ -f "$TRADEMINDS_DB" ]; then
    TABLE_COUNT=$(sqlite3 "$TRADEMINDS_DB" "SELECT COUNT(*) FROM sqlite_master WHERE type='table';" 2>/dev/null || echo "0")
    echo "$OK Database: $TRADEMINDS_DB ($TABLE_COUNT tables)"
else
    echo "$FAIL Database not found: $TRADEMINDS_DB"
    echo "  Run: cd $TRADEMINDS_DIR && python setup_db.py"
    exit 1
fi

# 3. Ollama running
if curl -s http://localhost:11434/api/tags > /dev/null 2>&1; then
    MODEL_COUNT=$(curl -s http://localhost:11434/api/tags | python3 -c "import sys,json; print(len(json.load(sys.stdin).get('models',[])))" 2>/dev/null || echo "?")
    echo "$OK Ollama: running ($MODEL_COUNT models loaded)"
    curl -s http://localhost:11434/api/tags | python3 -c "
import sys, json
models = json.load(sys.stdin).get('models', [])
for m in models[:8]:
    name = m.get('name', '?')
    size = m.get('size', 0)
    size_gb = round(size / 1e9, 1)
    print(f'       - {name} ({size_gb}GB)')
" 2>/dev/null || true
else
    echo "$WARN Ollama: not running"
    echo "  Start with: ollama serve &"
    echo "  Continuing anyway — cloud models still available"
fi

# 3b. MLX server — disabled (Chekov now routes through Ollama qwen3:8b)
MLX_PID=""

# 4. Crew module
if [ -f "$TRADEMINDS_DIR/crew/__init__.py" ]; then
    echo "$OK Crew module: installed"
else
    echo "$WARN Crew module: not found at $TRADEMINDS_DIR/crew/"
fi

# 5. Migration tables
CREW_TABLES=$(sqlite3 "$TRADEMINDS_DB" "SELECT COUNT(*) FROM sqlite_master WHERE type='table' AND name IN ('portfolios','portfolio_positions','crew_strategies','crew_runs');" 2>/dev/null || echo "0")
if [ "$CREW_TABLES" -eq 4 ]; then
    echo "$OK Migration tables: all 4 present"
    PORTFOLIO_COUNT=$(sqlite3 "$TRADEMINDS_DB" "SELECT COUNT(*) FROM portfolios;" 2>/dev/null || echo "0")
    echo "       - $PORTFOLIO_COUNT portfolios seeded"
else
    echo "$WARN Migration tables: $CREW_TABLES/4 present"
    echo "  Run: cd $TRADEMINDS_DIR && python migrations/001_crew_and_portfolios.py"
fi

# 6. Python environment
echo ""
if [ -x "$VENV_ARENA" ]; then
    ARENA_PY_VER=$("$VENV_ARENA" --version 2>/dev/null)
    if "$VENV_ARENA" -c "import fastapi, uvicorn" >/dev/null 2>&1; then
        echo "$OK venv: $ARENA_PY_VER [$VENV_ARENA]"
    else
        echo "$WARN venv: $ARENA_PY_VER [$VENV_ARENA] missing FastAPI/Uvicorn"
    fi
else
    echo "$FAIL venv: not found at $VENV_ARENA"
    echo "  Expected: $TRADEMINDS_DIR/venv/bin/python3"
    exit 1
fi

# ---------------------------------------------------------------------------
# Start both servers
# ---------------------------------------------------------------------------
start_servers() {
    echo ""
    echo "========================================"
    echo "  Starting Servers"
    echo "========================================"
    echo ""

    cd "$TRADEMINDS_DIR"

    # USS TradeMinds — port 8080 (single server)
    echo "$INFO Starting USS TradeMinds on :8080..."
    "$VENV_ARENA" main.py > "$TRADEMINDS_DIR/logs/arena.log" 2>&1 &
    ARENA_PID=$!
    echo "$INFO TradeMinds PID: $ARENA_PID (log: logs/arena.log)"

    local arena_ready=0
    local attempt
    for attempt in {1..20}; do
        if ! kill -0 "$ARENA_PID" 2>/dev/null; then
            echo "$FAIL TradeMinds exited before binding to :8080"
            tail -n 40 "$TRADEMINDS_DIR/logs/arena.log" || true
            exit 1
        fi
        if lsof -nP -iTCP:8080 -sTCP:LISTEN >/dev/null 2>&1; then
            arena_ready=1
            break
        fi
        sleep 1
    done
    if [ "$arena_ready" -eq 1 ]; then
        echo "$OK TradeMinds listening on 127.0.0.1:8080"
    else
        echo "$FAIL TradeMinds did not bind to 127.0.0.1:8080 within 20s"
        tail -n 60 "$TRADEMINDS_DIR/logs/arena.log" || true
        exit 1
    fi

    echo ""
    echo "  USS TradeMinds:   http://localhost:8080"
    echo "  API Docs:         http://localhost:8080/docs"
    echo ""
}

# ---------------------------------------------------------------------------
# Launch modes
# ---------------------------------------------------------------------------
echo ""
echo "========================================"
echo "  Launching"
echo "========================================"
echo ""
echo "$INFO Reminder: Press Shift+Tab to cycle to auto mode"
echo ""

MODE="${1:-dev}"

case "$MODE" in
    --servers)
        echo "$INFO Mode: Servers Only (no Claude Code)"
        start_servers
        echo "$INFO Both servers running. Press Ctrl+C to stop."
        wait
        ;;
    --crew)
        echo "$INFO Mode: CrewAI Strategy Pipeline"
        start_servers
        exec claude --enable-auto-mode \
            -p "You are aboard the USS TradeMinds. Server running on :8080. The CrewAI strategy-writing crew is ready. Available commands: run_crew() for full pipeline, CrewPipeline().run_scout_only() for quick scan, CrewPipeline().run_sunday_review() for Sunday special. API endpoints at /api/crew/* and /api/portfolios/*. CRITICAL: Never auto-trade Webull (is_human=1). Default unproven strategies to Alpaca Paper."
        ;;
    --scout)
        echo "$INFO Mode: Scout Quick Scan"
        start_servers
        exec claude --enable-auto-mode \
            -p "Run a quick scout scan: from crew.pipeline import CrewPipeline; result = CrewPipeline().run_scout_only(); print the top opportunities found."
        ;;
    *)
        echo "$INFO Mode: Full Dev Mode"
        start_servers
        exec claude
        ;;
esac
