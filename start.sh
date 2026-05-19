#!/data/data/com.termux/files/usr/bin/bash
# start.sh — TRENDRIDER tmux split screen
# Top (30%): watcher.py — git bridge + trader output piped through
# Bottom (70%): watcher log only if you want a second view
# NOTE: trader output now prints directly through watcher — no tmux needed.
# Run this if you want tmux. Otherwise just: python watcher.py

SESSION="trendrider"
REPO="$(cd "$(dirname "$0")" && pwd)"

# Get terminal size without tput (Termux compatible)
read ROWS COLS < <(stty size 2>/dev/null || echo "50 180")
ROWS=${ROWS:-50}
COLS=${COLS:-180}

# Kill any existing session
tmux kill-session -t "$SESSION" 2>/dev/null || true

# New session with explicit size
tmux new-session -d -s "$SESSION" -x "$COLS" -y "$ROWS"

# Only one pane needed — trader output flows through watcher now
tmux send-keys -t "$SESSION:0" "cd $REPO && python watcher.py" Enter

tmux attach -t "$SESSION"
