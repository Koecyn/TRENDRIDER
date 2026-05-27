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


def backfill_local(verbose=True):
    """
    Fetch 1m + 1h candles from Binance.US and save to /tmp/trendrider/ (tmpfs).
    Never writes to the repo data/raw/ or to .git.
    Android doesn't back up /tmp/ — cleared on reboot, RAM only.

    Gap-aware: fetches exactly what's missing.
      20-min gap  →  ~20  × 1m bars  (fast)
      20-day gap  →  ~28 800 × 1m bars, paginated automatically in 1 000-bar chunks

    Returns (n_1m, n_1h) new bars fetched.
    """
    def log(m):
        if verbose: print(f"[candles] {m}", flush=True)

    tmp_dir = Path(os.environ.get('TMPDIR', '/tmp')) / 'trendrider'
    tmp_dir.mkdir(parents=True, exist_ok=True)

    now_ms = int(time.time() * 1000)
    end_ms = now_ms - 60_000   # exclude the live minute

    def _load_local(fname):
        p = tmp_dir / fname
        if p.exists():
            try:
                with gzip.open(p, "rt") as f:
                    return json.loads(f.read())
            except Exception:
                pass
        return []

    def _save_local(fname, data):
        p = tmp_dir / fname
        with gzip.open(p, "wt") as f:
            json.dump(data, f)

    def _merge(existing, new):
        merged = {k[0]: k for k in existing}
        for k in new: merged[k[0]] = k
        return sorted(merged.values(), key=lambda k: k[0])

    # Determine 1m start.
    # If local file exists: fetch from last bar forward (exact gap).
    # If no local file: default 7 days (enough historical context for the engine).
    # _fetch_klines paginates in 1 000-bar chunks → handles any size gap.
    existing_1m = _load_local(FNAME_1M)
    start_1m = (existing_1m[-1][0] + 60_000 if existing_1m
                else end_ms - 7 * 24 * 60 * 60_000)   # 7 days default

    # Determine 1h start.
    existing_1h = _load_local("BTCUSDT_1h.json.gz")
    start_1h = (existing_1h[-1][0] + 3_600_000 if existing_1h
                else end_ms - 90 * 24 * 3_600_000)     # 90 days default
    start_1h = (start_1h // 3_600_000) * 3_600_000     # round to hour boundary

    gap_min = max(0, int((end_ms - start_1m) / 60_000))
    gap_hr  = max(0, int((end_ms - start_1h) / 3_600_000))

    if gap_min == 0 and gap_hr == 0:
        log("candle data is current — nothing to fetch")
        return 0, 0

    log(f"fetching {gap_min} x 1m  +  {gap_hr} x 1h from Binance.US "
        f"(paginated in {MAX_BARS}-bar chunks) ...")

    bars_1m = _fetch_klines("1m", start_1m, end_ms) if gap_min > 0 else []
    bars_1h = _fetch_klines("1h", start_1h, end_ms) if gap_hr  > 0 else []

    combined_1m = _merge(existing_1m, bars_1m)
    combined_1h = _merge(existing_1h, bars_1h)

    _save_local(FNAME_1M,              combined_1m)
    _save_local("BTCUSDT_1h.json.gz",  combined_1h)

    log(f"saved → /tmp/trendrider/  "
        f"1m: {len(bars_1m)} new (total {len(combined_1m)})  "
        f"|  1h: {len(bars_1h)} new (total {len(combined_1h)})")
    return len(bars_1m), len(bars_1h)


if __name__ == "__main__":
    import argparse as _ap
    _p = _ap.ArgumentParser()
    _p.add_argument("--local", action="store_true",
                    help="save candles to local files (no git push)")
    _a = _p.parse_args()
    if _a.local:
        n1m, n1h = backfill_local()
        print(f"Done — {n1m} x 1m  {n1h} x 1h fetched.")
    else:
        n = backfill()
        print(f"Done — {n} bars fetched.")
