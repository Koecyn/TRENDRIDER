#!/data/data/com.termux/files/usr/bin/bash
# Quick command 2 — pull latest code + run wave scan
cd ~/TRENDRIDER
git fetch --depth 1 origin claude/hft-mean-reversion-strategy-pJ4YU 2>/dev/null
git reset --hard origin/claude/hft-mean-reversion-strategy-pJ4YU 2>/dev/null
python wave_scan.py --session 0 --signals
