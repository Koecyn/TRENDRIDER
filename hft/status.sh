#!/usr/bin/env bash
# status.sh — Show engine status and recent activity.
# Run from repo root: bash hft/status.sh [--follow]

cd "$(dirname "$0")/.."

PID_FILE="hft/hft.pid"
LOG="hft/logs/engine.log"

echo "══════════════════════════════════════════"
echo "  TRENDRIDER HFT — Engine Status"
echo "══════════════════════════════════════════"

if [[ -f "$PID_FILE" ]]; then
    PID=$(cat "$PID_FILE")
    if kill -0 "$PID" 2>/dev/null; then
        UPTIME=$(ps -o etime= -p "$PID" 2>/dev/null | xargs || echo "unknown")
        echo "  Status : RUNNING"
        echo "  PID    : $PID"
        echo "  Uptime : $UPTIME"
    else
        echo "  Status : STOPPED (stale PID $PID)"
    fi
else
    echo "  Status : STOPPED (no PID file)"
fi

echo ""

# Latest STATUS line from log
if [[ -f "$LOG" ]]; then
    LAST_STATUS=$(grep "STATUS" "$LOG" | tail -1)
    if [[ -n "$LAST_STATUS" ]]; then
        echo "  Last status: $LAST_STATUS"
    fi

    # Latest trade
    LAST_TRADE=$(grep -E "ENTER|CLOSE" "$LOG" | tail -1)
    if [[ -n "$LAST_TRADE" ]]; then
        echo "  Last trade:  $LAST_TRADE"
    fi

    echo ""
    echo "── Recent log (last 30 lines) ─────────────────"
    tail -30 "$LOG"
else
    echo "  No log file found at $LOG"
fi

echo ""

# Follow mode
if [[ "${1:-}" == "--follow" || "${1:-}" == "-f" ]]; then
    echo "── Following log (Ctrl+C to stop) ────────────"
    tail -f "$LOG"
fi
