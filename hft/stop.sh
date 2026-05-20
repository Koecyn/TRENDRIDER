#!/usr/bin/env bash
# stop.sh — Gracefully stop the HFT engine.
# Run from repo root: bash hft/stop.sh

set -euo pipefail
cd "$(dirname "$0")/.."

PID_FILE="hft/hft.pid"

if [[ ! -f "$PID_FILE" ]]; then
    echo "No PID file found — engine may not be running."
    exit 0
fi

PID=$(cat "$PID_FILE")

if ! kill -0 "$PID" 2>/dev/null; then
    echo "Process $PID not found — removing stale PID file."
    rm -f "$PID_FILE"
    exit 0
fi

echo "Stopping engine (PID $PID)..."
kill -TERM "$PID"

# Wait up to 10s for clean exit
for i in $(seq 1 10); do
    sleep 1
    if ! kill -0 "$PID" 2>/dev/null; then
        echo "Engine stopped."
        exit 0
    fi
done

echo "Engine did not stop cleanly — force killing."
kill -9 "$PID" 2>/dev/null || true
rm -f "$PID_FILE"
