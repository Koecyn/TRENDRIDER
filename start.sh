#!/data/data/com.termux/files/usr/bin/bash
# start.sh — launch TRENDRIDER in a tmux split screen
# Top pane  (30%): watcher.py  — git bridge, trigger commands
# Bottom pane (70%): trader output — price, wallet, positions, trades

SESSION="trendrider"
REPO="$(cd "$(dirname "$0")" && pwd)"

# Kill existing session cleanly
tmux kill-session -t "$SESSION" 2>/dev/null || true

# Create detached session sized to terminal
tmux new-session -d -s "$SESSION" -x "$(tput cols)" -y "$(tput lines)"

# Split: bottom pane gets 70%
tmux split-window -v -p 70 -t "$SESSION"

# Top pane (0): watcher — git bridge
tmux send-keys -t "$SESSION:0.0" \
    "cd $REPO && python watcher.py" Enter

# Bottom pane (1): trader log — tail -f so output scrolls naturally
tmux send-keys -t "$SESSION:0.1" \
    "cd $REPO && echo 'Waiting for trader to start…' && sleep 3 && tail -n 60 -f process.log" Enter

# Focus top pane and attach
tmux select-pane -t "$SESSION:0.0"
tmux attach -t "$SESSION"
