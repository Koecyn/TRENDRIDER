#!/data/data/com.termux/files/usr/bin/bash
# Quick command 2 — pull latest code + run scanner (pushes signals to repo)
cd ~/TRENDRIDER
git fetch --depth 1 origin claude/hft-mean-reversion-strategy-pJ4YU
git reset --hard origin/claude/hft-mean-reversion-strategy-pJ4YU
python scan_live.py
