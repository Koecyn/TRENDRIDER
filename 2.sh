#!/data/data/com.termux/files/usr/bin/bash
# Quick command 2 — pull latest code (scan now runs inside collect_raw.py / 1.sh)
cd ~/TRENDRIDER
git fetch --depth 1 origin claude/hft-mean-reversion-strategy-pJ4YU
git reset --hard origin/claude/hft-mean-reversion-strategy-pJ4YU
echo "Scan is now built into 1.sh (collect_raw.py)."
echo "Run 1.sh — signals stream live as each second of data arrives."
