#!/usr/bin/env python3
"""
pull_candles.py — backfill 1m OHLCV for the gap between last raw data and now.

Called automatically at collect_raw.py startup.
Only fetches candles that aren't already covered by the raw tick file.
If gap < 2 minutes: skips (collector will fill it live).
If gap > 1000 minutes: caps at 1000 bars (Binance API limit per call).

Each stored record: [open_time_ms, open, high, low, close, volume, taker_buy_vol]
Stored as data/raw/BTCUSDT_1m.json.gz on the data/raw branch.
"""

import gzip, json, os, subprocess, sys, time, urllib.request
from pathlib import Path

REPO       = Path(__file__).resolve().parent
BASE_URL   = "https://api.binance.us"
SYMBOL     = "BTCUSDT"
BRANCH     = "data/raw"
DATA_DIR   = "data/raw"
FNAME_1M   = "BTCUSDT_1m.json.gz"
FNAME_LIVE = "BTCUSDT_LIVE.jsonl.gz"
MIN_GAP_S  = 120   # skip if gap < 2 minutes
MAX_BARS   = 1000  # Binance API max per call


def _run(*a):
    return subprocess.run(list(a), capture_output=True, text=True, cwd=str(REPO))


def _last_ts_in_live():
    """Return the last timestamp (ms) in the live raw file, or None."""
    r = _run("git", "show", f"origin/{BRANCH}:{DATA_DIR}/{FNAME_LIVE}")
    if r.returncode != 0 or not r.stdout:
        return None
    try:
        data = gzip.decompress(r.stdout.encode("latin-1")).decode()
    except Exception:
        return None
    last_ts = None
    for ln in data.splitlines():
        if not ln:
            continue
        try:
            rec = json.loads(ln)
            ts = rec[1]
            if last_ts is None or ts > last_ts:
                last_ts = ts
        except Exception:
            pass
    return last_ts


def _existing_1m_end():
    """Return the last open_time_ms in the stored 1m candle file, or None."""
    r = _run("git", "show", f"origin/{BRANCH}:{DATA_DIR}/{FNAME_1M}")
    if r.returncode != 0 or not r.stdout:
        return None
    try:
        data = gzip.decompress(r.stdout.encode("latin-1")).decode()
        klines = json.loads(data)
        if klines:
            return klines[-1][0]  # open_time_ms of last bar
    except Exception:
        pass
    return None


def _fetch_klines(start_ms, end_ms):
    """Fetch 1m klines from start_ms to end_ms (both inclusive)."""
    bars = []
    cursor = start_ms
    while cursor < end_ms:
        limit = min(MAX_BARS, ((end_ms - cursor) // 60_000) + 1)
        if limit <= 0:
            break
        url = (f"{BASE_URL}/api/v3/klines"
               f"?symbol={SYMBOL}&interval=1m"
               f"&startTime={cursor}&endTime={end_ms}&limit={limit}")
        with urllib.request.urlopen(url, timeout=15) as resp:
            chunk = json.loads(resp.read())
        if not chunk:
            break
        for k in chunk:
            bars.append([int(k[0]), float(k[1]), float(k[2]),
                         float(k[3]), float(k[4]), float(k[5]), float(k[9])])
        last_open = chunk[-1][0]
        if last_open <= cursor:
            break
        cursor = last_open + 60_000
    return bars


def _push_1m(klines):
    """Append new bars to existing 1m file and push via git plumbing."""
    # Load existing bars if any
    existing = []
    r = _run("git", "show", f"origin/{BRANCH}:{DATA_DIR}/{FNAME_1M}")
    if r.returncode == 0 and r.stdout:
        try:
            existing = json.loads(
                gzip.decompress(r.stdout.encode("latin-1")).decode()
            )
        except Exception:
            existing = []

    # Merge: deduplicate by open_time_ms, keep sorted
    merged = {k[0]: k for k in existing}
    for k in klines:
        merged[k[0]] = k
    combined = sorted(merged.values(), key=lambda k: k[0])

    gz = gzip.compress(json.dumps(combined).encode(), compresslevel=6)

    # git plumbing push
    blob = subprocess.run(
        ["git", "hash-object", "-w", "--stdin"],
        input=gz, capture_output=True, cwd=str(REPO)
    )
    blob_sha = blob.stdout.decode().strip()
    if not blob_sha:
        raise RuntimeError("hash-object failed")

    tmp_idx = REPO / ".git" / "candle_push.idx"
    env = {**os.environ, "GIT_INDEX_FILE": str(tmp_idx)}
    parent_r = _run("git", "rev-parse", f"origin/{BRANCH}")
    parent = parent_r.stdout.strip()
    if parent:
        subprocess.run(["git", "read-tree", f"origin/{BRANCH}"],
                       capture_output=True, cwd=str(REPO), env=env)

    rel = f"{DATA_DIR}/{FNAME_1M}"
    subprocess.run(["git", "update-index", "--add",
                    "--cacheinfo", f"100644,{blob_sha},{rel}"],
                   capture_output=True, cwd=str(REPO), env=env)

    tree_r = subprocess.run(["git", "write-tree"],
                             capture_output=True, text=True,
                             cwd=str(REPO), env=env)
    tree = tree_r.stdout.strip()
    tmp_idx.unlink(missing_ok=True)
    if not tree:
        raise RuntimeError("write-tree failed")

    msg = f"candles 1m {int(time.time())} bars={len(combined)}"
    commit_r = subprocess.run(
        ["git", "commit-tree", tree, "-p", parent, "-m", msg],
        capture_output=True, text=True, cwd=str(REPO)
    )
    commit_sha = commit_r.stdout.strip()
    if not commit_sha:
        raise RuntimeError("commit-tree failed")

    push_r = _run("git", "push", "origin",
                  f"{commit_sha}:refs/heads/{BRANCH}")
    if push_r.returncode != 0:
        raise RuntimeError(f"push failed: {push_r.stderr.strip()}")

    return len(combined), len(klines)


def backfill(verbose=True):
    """
    Main entry point. Call at collector startup.
    Returns number of bars fetched (0 if skipped).
    """
    def log(m):
        if verbose: print(f"[candles] {m}", flush=True)

    subprocess.run(["git", "fetch", "origin", BRANCH],
                   capture_output=True, cwd=str(REPO))

    now_ms = int(time.time() * 1000)

    # Determine gap start: later of (last raw ts) or (last 1m bar end)
    last_raw = _last_ts_in_live()
    last_1m  = _existing_1m_end()

    if last_raw is None and last_1m is None:
        log("no existing data — skipping backfill")
        return 0

    # Gap starts at the later of the two known endpoints
    candidates = [t for t in [last_raw, last_1m] if t is not None]
    gap_start_ms = max(candidates)
    # Round up to next full minute boundary
    gap_start_ms = ((gap_start_ms // 60_000) + 1) * 60_000

    gap_s = (now_ms - gap_start_ms) / 1000
    if gap_s < MIN_GAP_S:
        log(f"gap {gap_s:.0f}s < {MIN_GAP_S}s — nothing to backfill")
        return 0

    gap_bars = int(gap_s / 60)
    log(f"gap {gap_s/60:.1f}m ({gap_bars} bars) — fetching 1m candles ...")

    klines = _fetch_klines(gap_start_ms, now_ms - 60_000)  # exclude live minute
    if not klines:
        log("no bars returned")
        return 0

    total, new = _push_1m(klines)
    log(f"pushed {new} new bars  (total stored: {total})")
    return new


if __name__ == "__main__":
    n = backfill()
    print(f"Done — {n} bars fetched.")
