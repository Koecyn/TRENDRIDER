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


def _fetch_klines(interval, start_ms, end_ms):
    """Fetch klines for given interval from start_ms to end_ms."""
    step_ms = 3_600_000 if interval == "1h" else 60_000
    bars = []
    cursor = start_ms
    while cursor < end_ms:
        limit = min(MAX_BARS, ((end_ms - cursor) // step_ms) + 1)
        if limit <= 0:
            break
        url = (f"{BASE_URL}/api/v3/klines"
               f"?symbol={SYMBOL}&interval={interval}"
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
        cursor = last_open + step_ms
    return bars


def _push_candles(bars_1m, bars_1h):
    """Push both 1m and 1h files to data/raw in a single commit."""
    def _load_existing(fname):
        r = _run("git", "show", f"origin/{BRANCH}:{DATA_DIR}/{fname}")
        if r.returncode == 0 and r.stdout:
            try:
                return json.loads(gzip.decompress(r.stdout.encode("latin-1")).decode())
            except Exception:
                pass
        return []

    def _merge(existing, new):
        merged = {k[0]: k for k in existing}
        for k in new: merged[k[0]] = k
        return sorted(merged.values(), key=lambda k: k[0])

    combined_1m = _merge(_load_existing(FNAME_1M), bars_1m)
    combined_1h = _merge(_load_existing("BTCUSDT_1h.json.gz"), bars_1h)

    def _blob(data):
        gz = gzip.compress(json.dumps(data).encode(), compresslevel=6)
        r = subprocess.run(["git","hash-object","-w","--stdin"],
                           input=gz, capture_output=True, cwd=str(REPO))
        return r.stdout.decode().strip(), gz

    blob_1m, gz_1m = _blob(combined_1m)
    blob_1h, gz_1h = _blob(combined_1h)
    if not blob_1m or not blob_1h:
        raise RuntimeError("hash-object failed")

    tmp_idx = REPO / ".git" / "candle_push.idx"
    env = {**os.environ, "GIT_INDEX_FILE": str(tmp_idx)}
    parent_r = _run("git", "rev-parse", f"origin/{BRANCH}")
    parent = parent_r.stdout.strip()
    if parent:
        subprocess.run(["git","read-tree",f"origin/{BRANCH}"],
                       capture_output=True, cwd=str(REPO), env=env)

    for fname, blob in [(FNAME_1M, blob_1m), ("BTCUSDT_1h.json.gz", blob_1h)]:
        subprocess.run(["git","update-index","--add",
                        "--cacheinfo", f"100644,{blob},{DATA_DIR}/{fname}"],
                       capture_output=True, cwd=str(REPO), env=env)

    tree_r = subprocess.run(["git","write-tree"], capture_output=True,
                             text=True, cwd=str(REPO), env=env)
    tree = tree_r.stdout.strip()
    tmp_idx.unlink(missing_ok=True)
    if not tree: raise RuntimeError("write-tree failed")

    msg = (f"candles 1m={len(combined_1m)} 1h={len(combined_1h)} "
           f"ts={int(time.time())}")
    commit_r = subprocess.run(
        ["git","commit-tree", tree, "-p", parent, "-m", msg],
        capture_output=True, text=True, cwd=str(REPO))
    commit_sha = commit_r.stdout.strip()
    if not commit_sha: raise RuntimeError("commit-tree failed")

    push_r = _run("git","push","origin",f"{commit_sha}:refs/heads/{BRANCH}")
    if push_r.returncode != 0:
        raise RuntimeError(f"push failed: {push_r.stderr.strip()}")

    return len(combined_1m), len(combined_1h), len(bars_1m), len(bars_1h)


def backfill(verbose=True):
    """
    Main entry point. Call at collector startup.
    Fetches 1m AND 1h candles covering the gap since last raw data.
    Returns number of new 1m bars fetched (0 if skipped).
    """
    def log(m):
        if verbose: print(f"[candles] {m}", flush=True)

    subprocess.run(["git","fetch","origin", BRANCH],
                   capture_output=True, cwd=str(REPO))

    now_ms = int(time.time() * 1000)

    last_raw = _last_ts_in_live()
    last_1m  = _existing_1m_end()

    if last_raw is None and last_1m is None:
        log("no existing data — skipping backfill")
        return 0

    candidates   = [t for t in [last_raw, last_1m] if t is not None]
    gap_start_ms = max(candidates)
    gap_start_ms = ((gap_start_ms // 60_000) + 1) * 60_000

    gap_s = (now_ms - gap_start_ms) / 1000
    if gap_s < MIN_GAP_S:
        log(f"gap {gap_s:.0f}s < {MIN_GAP_S}s — nothing to backfill")
        return 0

    gap_min = int(gap_s / 60)
    gap_hr  = max(1, gap_min // 60)
    log(f"gap {gap_s/3600:.2f}h — fetching {gap_min} x 1m  +  {gap_hr} x 1h ...")

    end_ms  = now_ms - 60_000   # exclude the live minute
    bars_1m = _fetch_klines("1m",  gap_start_ms, end_ms)
    # 1h: round gap_start down to hour boundary
    h_start = (gap_start_ms // 3_600_000) * 3_600_000
    bars_1h = _fetch_klines("1h",  h_start, end_ms)

    if not bars_1m and not bars_1h:
        log("no bars returned")
        return 0

    t1m, t1h, n1m, n1h = _push_candles(bars_1m, bars_1h)
    log(f"pushed  1m: {n1m} new (total {t1m})  |  1h: {n1h} new (total {t1h})")
    return n1m


if __name__ == "__main__":
    n = backfill()
    print(f"Done — {n} bars fetched.")
