#!/data/data/com.termux/files/usr/bin/bash
# cleanup_git.sh — one-time prune of data/raw blobs from local .git
#
# The data/raw branch accumulated large git object blobs from past raw data
# pushes.  These bloat the LOCAL .git/objects/ directory and get backed up by
# Android alongside your repo code.
#
# This script removes them from the LOCAL .git only.  The remote (GitHub) is
# untouched — your backed-up data stays on GitHub.
#
# After running, the local .git will only contain code commits + signal files.
# Run once.  Subsequent sessions won't re-bloat because:
#   • collect_raw.py runs 'git prune --expire=now' every ~5 min after each push
#   • wave_scan.py and scan_live.py never 'git fetch origin data/raw' locally

set -e
cd ~/TRENDRIDER

echo "=== .git size BEFORE ==="
du -sh .git

# 1. Remove the local remote-tracking reference for data/raw.
#    This makes ALL historical data blobs unreachable locally.
git update-ref -d refs/remotes/origin/data/raw 2>/dev/null || true

# 2. Remove any packed refs pointing to data/raw
git pack-refs --all 2>/dev/null || true

# 3. Aggressive garbage collection — removes all unreachable objects now.
#    This is the step that actually frees the disk space.
git gc --aggressive --prune=now

echo ""
echo "=== .git size AFTER ==="
du -sh .git
echo ""
echo "Done.  Run collect.sh (alias 1) to reconnect the collector."
echo "The collector will push fresh data/raw to GitHub on its next cycle."
