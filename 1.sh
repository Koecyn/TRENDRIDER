#!/data/data/com.termux/files/usr/bin/bash
# Quick command 1 — pull latest code, then start collector
cd ~/TRENDRIDER
git fetch --depth 1 origin claude/hft-mean-reversion-strategy-pJ4YU
git reset --hard FETCH_HEAD
python collect_raw.py
