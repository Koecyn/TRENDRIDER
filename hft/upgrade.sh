#!/usr/bin/env bash
# upgrade.sh — Pull latest code and hot-reload the running engine.
#
# If the engine is running:  git pull → SIGUSR1 (modules swap in <1s, WebSocket stays alive)
# If the engine is stopped:  git pull → start it fresh
#
# Run from repo root: bash hft/upgrade.sh

set -euo pipefail
cd "$(dirname "$0")/.."

BRANCH="claude/hft-mean-reversion-strategy-pJ4YU"
PID_FILE="hft/hft.pid"

echo "── Fetching latest code from $BRANCH ──"
git fetch origin "$BRANCH"
git reset --hard "origin/$BRANCH"

if [[ -f "$PID_FILE" ]]; then
    PID=$(cat "$PID_FILE")
    if kill -0 "$PID" 2>/dev/null; then
        echo "Engine running (PID $PID) — sending hot-reload signal"
        kill -USR1 "$PID"
        echo "Done. Modules will reload within 1s. No downtime, position preserved."
        echo "Watch: bash hft/status.sh"
        exit 0
    else
        echo "Stale PID file — engine not running. Starting fresh."
        rm -f "$PID_FILE"
    fi
fi

echo "Engine not running — starting now."
bash hft/start.sh
