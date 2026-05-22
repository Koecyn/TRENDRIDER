#!/bin/bash
# start_live.sh — supervise physics_live.py and auto-restart on exit.
#
# physics/config.py  → hot-reloads in-process (no restart needed)
# physics/htf.py     → hot-reloads in-process (no restart needed)
# physics_live.py    → engine detects the change, exits cleanly, we restart
#
# Usage (Termux foreground):
#   bash start_live.sh
#
# Usage (Termux background, survives session close):
#   nohup bash start_live.sh >> physics_live.log 2>&1 &

cd "$(dirname "$0")"

while true; do
    echo "[$(date '+%H:%M:%S')] Starting physics_live.py..."
    python physics_live.py
    CODE=$?
    if [ $CODE -eq 0 ]; then
        echo "[$(date '+%H:%M:%S')] Clean exit (new code deployed) — restarting in 2s..."
        sleep 2
    else
        echo "[$(date '+%H:%M:%S')] Crashed (exit=$CODE) — restarting in 5s..."
        sleep 5
    fi
done
