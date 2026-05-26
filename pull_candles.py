#!/usr/bin/env python3
"""
pull_candles.py — fetch 1h and 1m OHLCV from Binance.US REST, commit to data/raw.

Pulls:
  - 24 x 1h candles  (last 24 hours)
  - 1440 x 1m candles (last 24 hours)

Stores as data/raw/BTCUSDT_1h.json.gz and data/raw/BTCUSDT_1m.json.gz
on the data/raw branch, alongside the existing BTCUSDT_LIVE.jsonl.gz.

Each record: [open_time_ms, open, high, low, close, volume, taker_buy_base_vol]

Usage: python pull_candles.py
"""

import gzip, json, os, subprocess, sys, time, urllib.request

REPO        = os.path.dirname(os.path.abspath(__file__))
BASE_URL    = "https://api.binance.us"
SYMBOL      = "BTCUSDT"
BRANCH      = "data/raw"
DATA_DIR    = "data/raw"

INTERVALS = [
    ("1h",  24,   "BTCUSDT_1h.json.gz"),
    ("1m",  1440, "BTCUSDT_1m.json.gz"),
]


def fetch_klines(interval, limit):
    url = (f"{BASE_URL}/api/v3/klines"
           f"?symbol={SYMBOL}&interval={interval}&limit={limit}")
    print(f"  GET {url}", flush=True)
    with urllib.request.urlopen(url, timeout=15) as r:
        raw = json.loads(r.read())
    # raw[i] = [open_time, open, high, low, close, volume, close_time,
    #           quote_vol, trades, taker_buy_base, taker_buy_quote, ignore]
    return [[int(k[0]), float(k[1]), float(k[2]), float(k[3]),
             float(k[4]), float(k[5]), float(k[9])] for k in raw]


def git_plumbing_push(filename, data_gz):
    """Write file to data/raw branch via git plumbing (no worktree needed)."""
    # hash-object the new blob
    blob = subprocess.run(
        ["git", "hash-object", "-w", "--stdin"],
        input=data_gz, capture_output=True, cwd=REPO
    )
    if blob.returncode != 0:
        raise RuntimeError(f"hash-object failed: {blob.stderr.decode()}")
    blob_sha = blob.stdout.decode().strip()

    # Get current tree of data/raw branch
    tree_out = subprocess.run(
        ["git", "ls-tree", f"origin/{BRANCH}", DATA_DIR + "/"],
        capture_output=True, text=True, cwd=REPO
    )
    # Build mktree input: preserve existing files, update/add ours
    entries = {}
    for line in tree_out.stdout.splitlines():
        mode, typ, sha, path = line.split(None, 3)
        entries[path] = f"{mode} {typ} {sha}\t{path}"

    path_in_tree = f"{DATA_DIR}/{filename}"
    entries[path_in_tree] = f"100644 blob {blob_sha}\t{path_in_tree}"

    mktree_in = "\n".join(entries.values()) + "\n"
    tree = subprocess.run(
        ["git", "mktree"],
        input=mktree_in.encode(), capture_output=True, cwd=REPO
    )
    if tree.returncode != 0:
        raise RuntimeError(f"mktree failed: {tree.stderr.decode()}")
    tree_sha = tree.stdout.decode().strip()

    # commit-tree
    parent = subprocess.run(
        ["git", "rev-parse", f"origin/{BRANCH}"],
        capture_output=True, text=True, cwd=REPO
    ).stdout.strip()

    msg = f"candles {filename} {int(time.time())}"
    commit = subprocess.run(
        ["git", "commit-tree", tree_sha, "-p", parent, "-m", msg],
        capture_output=True, text=True, cwd=REPO
    )
    if commit.returncode != 0:
        raise RuntimeError(f"commit-tree failed: {commit.stderr.decode()}")
    commit_sha = commit.stdout.strip()

    # push
    push = subprocess.run(
        ["git", "push", "origin", f"{commit_sha}:refs/heads/{BRANCH}"],
        capture_output=True, text=True, cwd=REPO
    )
    if push.returncode != 0:
        raise RuntimeError(f"push failed: {push.stderr.decode()}")
    print(f"  pushed {commit_sha[:8]} → {BRANCH}", flush=True)


def main():
    print("Fetching origin/data/raw ...", flush=True)
    subprocess.run(["git", "fetch", "origin", BRANCH],
                   capture_output=True, cwd=REPO)

    for interval, limit, fname in INTERVALS:
        print(f"\n{interval} candles ({limit} bars) → {fname}")
        try:
            klines = fetch_klines(interval, limit)
            gz = gzip.compress(json.dumps(klines).encode(), compresslevel=6)
            print(f"  {len(klines)} bars  {len(gz)//1024} kB gz")
            git_plumbing_push(fname, gz)
        except Exception as e:
            print(f"  ERROR: {e}", file=sys.stderr)
            sys.exit(1)

    print("\nDone.")


if __name__ == "__main__":
    main()
