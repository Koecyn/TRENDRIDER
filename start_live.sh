#!/bin/bash
# start_live.sh — supervise physics_live.py and auto-restart on exit.
#
# physics/config.py  → hot-reloads in-process (no restart needed)
# physics/htf.py     → hot-reloads in-process (no restart needed)
# physics_live.py    → engine detects the change, exits cleanly, we restart
#
# Ctrl+C / SIGTERM → stops the supervisor cleanly (no restart)
#
# Usage (Termux foreground):
#   bash start_live.sh
#
# Usage (Termux background, survives session close):
#   nohup bash start_live.sh >> physics_live.log 2>&1 &

cd "$(dirname "$0")"

CHILD_PID=""
STOPPING=0

_stop() {
    STOPPING=1
    echo "[$(date '+%H:%M:%S')] Caught signal — shutting down..."
    [ -n "$CHILD_PID" ] && kill "$CHILD_PID" 2>/dev/null
}

trap _stop INT TERM

while [ "$STOPPING" -eq 0 ]; do
    echo "[$(date '+%H:%M:%S')] Starting physics_live.py..."
    python physics_live.py &
    CHILD_PID=$!
    wait "$CHILD_PID"
    CODE=$?
    CHILD_PID=""

    [ "$STOPPING" -eq 1 ] && break

    if [ $CODE -eq 0 ]; then
        echo "[$(date '+%H:%M:%S')] Clean exit (new code deployed) — restarting in 2s..."
        sleep 2
    elif [ $CODE -eq 130 ] || [ $CODE -eq 143 ]; then
        # 130 = SIGINT, 143 = SIGTERM — user killed child directly, still stop
        echo "[$(date '+%H:%M:%S')] Killed by signal — stopping supervisor"
        break
    else
        echo "[$(date '+%H:%M:%S')] Crashed (exit=$CODE) — restarting in 5s..."
        sleep 5
    fi
done

echo "[$(date '+%H:%M:%S')] Supervisor stopped"
