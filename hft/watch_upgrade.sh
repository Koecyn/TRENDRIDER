#!/usr/bin/env bash
# watch_upgrade.sh — Auto-upgrade the engine whenever new commits land on the branch.
#
# Polls GitHub every POLL_SECONDS. On new commit: git pull → SIGUSR1 hot-reload.
# Run in a separate terminal: bash hft/watch_upgrade.sh
#
# Stop with Ctrl+C.

set -euo pipefail
cd "$(dirname "$0")/.."

BRANCH="claude/hft-mean-reversion-strategy-pJ4YU"
POLL_SECONDS=30
PID_FILE="hft/hft.pid"

echo "Watching $BRANCH for changes (polling every ${POLL_SECONDS}s)..."
echo "Press Ctrl+C to stop the watcher (engine keeps running)."
echo ""

LAST_COMMIT=$(git rev-parse "origin/$BRANCH" 2>/dev/null || echo "none")

while true; do
    sleep "$POLL_SECONDS"

    git fetch origin "$BRANCH" -q 2>/dev/null || {
        echo "[$(date +%H:%M:%S)] fetch failed — retrying in ${POLL_SECONDS}s"
        continue
    }

    NEW_COMMIT=$(git rev-parse "origin/$BRANCH")

    if [[ "$NEW_COMMIT" != "$LAST_COMMIT" ]]; then
        echo "[$(date +%H:%M:%S)] New commit detected: ${NEW_COMMIT:0:8}"
        LAST_COMMIT="$NEW_COMMIT"
        bash hft/upgrade.sh
    fi
done
