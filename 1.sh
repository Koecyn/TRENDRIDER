#!/data/data/com.termux/files/usr/bin/bash
# Quick command 1 — pull latest code, then start collector
cd ~/TRENDRIDER
git update-ref -d refs/remotes/origin/data/raw 2>/dev/null || true
git update-ref -d refs/remotes/origin/data/signals 2>/dev/null || true
git fetch --depth 1 --prune origin claude/hft-mean-reversion-strategy-pJ4YU
git reset --hard FETCH_HEAD
python collect_raw.py
