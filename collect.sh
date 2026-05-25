#!/data/data/com.termux/files/usr/bin/bash
cd ~/TRENDRIDER
git fetch origin claude/hft-mean-reversion-strategy-pJ4YU
git reset --hard origin/claude/hft-mean-reversion-strategy-pJ4YU
python collect_raw.py
