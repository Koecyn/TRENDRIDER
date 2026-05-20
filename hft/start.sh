#!/usr/bin/env bash
# start.sh — Launch the HFT paper engine.
# Writes PID to hft/hft.pid and tees all output to hft/logs/engine.log.
# Run from repo root: bash hft/start.sh

set -euo pipefail
cd "$(dirname "$0")/.."

LOG_DIR="hft/logs"
mkdir -p "$LOG_DIR"

PID_FILE="hft/hft.pid"
if [[ -f "$PID_FILE" ]]; then
    PID=$(cat "$PID_FILE")
    if kill -0 "$PID" 2>/dev/null; then
        echo "Engine already running (PID $PID). Use: bash hft/upgrade.sh"
        exit 0
    else
        echo "Stale PID file removed."
        rm -f "$PID_FILE"
    fi
fi

SYMBOL="${1:-}"
EXTRA_ARGS=""
if [[ -n "$SYMBOL" ]]; then
    EXTRA_ARGS="--symbol $SYMBOL"
fi

echo "Starting HFT engine... (logs → $LOG_DIR/engine.log)"
exec python hft/main.py $EXTRA_ARGS 2>&1 | tee -a "$LOG_DIR/engine.log"
