#!/usr/bin/env python3
"""
collect_raw.py — pipe WebSocket stream into a bounded in-memory deque.

Hot path : message → deque.append(line).  No writes to phone flash storage.
Push loop: every PUSH_S seconds —
             1. Snapshot deque → /tmp/trendrider/BTCUSDT_LIVE.jsonl.gz (tmpfs/RAM)
                wave_scan.py reads from here directly — no git fetch needed.
             2. Push that same snapshot to GitHub data/raw branch (remote backup).
                After push, the 3 local git objects (blob/tree/commit) are
                unreachable (no local data/raw tracking ref) and are cleaned up
                by periodic git gc --auto.

.git object hygiene:
  - NEVER run 'git fetch origin data/raw' on the phone — that would make all
    historical blobs reachable locally and block pruning.
  - After GC_PRUNE_EVERY cycles, run 'git prune --expire=now' to sweep the
    loose unreachable objects created by each push cycle.
  - Android never backs up /tmp/ (tmpfs — cleared on reboot, RAM only).

Deque contract:
  maxlen = WINDOW_LINES (~15k) → oldest records evicted automatically.
  Thread-safe: append + list() snapshot both under _deque_lock.

Trade line  : ["T", ts_ms, price_cents, qty_units, side]
Depth line  : ["D", ts_ms, [[p_c,q_u],...bids], [[p_c,q_u],...asks]]

Usage: python collect_raw.py
"""

import asyncio
import collections
import gc
import gzip
import json
import os
import resource
import signal
import subprocess
import sys
import threading
import time
from pathlib import Path

try:
    import aiohttp
except ImportError:
    print("ERROR: pip install aiohttp"); sys.exit(1)

REPO      = Path(__file__).resolve().parent
TMP_DIR   = Path('/tmp/trendrider')            # tmpfs — RAM only
GZ_FILE   = TMP_DIR / 'BTCUSDT_LIVE.jsonl.gz' # wave_scan reads this
DATA_BRANCH   = "data/raw"
GIT_TREE_PATH = "data/raw/BTCUSDT_LIVE.jsonl.gz"  # path inside git tree
TMP_IDX       = REPO / ".git" / "data_push.idx"

WS_URL = ("wss://stream.binance.us:9443/stream"
          "?streams=btcusdt@depth20@100ms/btcusdt@aggTrade")

# Bounded deque: ~96 min at 2 records/sec = 11 520 entries; 15k gives headroom.
WINDOW_LINES   = 15_000
PUSH_S         = 2        # write to tmpfs + git push every N seconds
GC_EVERY       = 60       # gc.collect() every N push cycles
LOG_MEM_EVERY  = 300      # log RSS every N push cycles
GC_PRUNE_EVERY = 150      # git prune every N cycles (~5 min) to sweep loose objects

_raw_deque  = collections.deque(maxlen=WINDOW_LINES)
_deque_lock = threading.Lock()

G='\033[92m'; R='\033[91m'; Y='\033[93m'; Z='\033[0m'
def log(m, c=Z): print(f"{c}[raw] {m}{Z}", flush=True)

def _run(*a, env=None):
    return subprocess.run(list(a), capture_output=True, text=True,
                          cwd=str(REPO), env=env)


# ── git push (pushes to GitHub — never fetches data/raw locally) ──────────────

def _git_push_snapshot(gz_path: Path) -> bool:
    """
    Push gz_path to DATA_BRANCH at GIT_TREE_PATH using git plumbing.
    Does NOT update any local tracking ref — the 3 objects created (blob/tree/commit)
    remain unreachable locally and are swept by the periodic git prune.
    """
    r = _run('git', 'hash-object', '-w', str(gz_path))
    blob = r.stdout.strip()
    if not blob:
        return False

    env = {**os.environ, 'GIT_INDEX_FILE': str(TMP_IDX)}
    parent_r = _run('git', 'rev-parse', f'origin/{DATA_BRANCH}')
    parent   = parent_r.stdout.strip()
    if parent:
        _run('git', 'read-tree', f'origin/{DATA_BRANCH}', env=env)

    _run('git', 'update-index', '--add',
         '--cacheinfo', f'100644,{blob},{GIT_TREE_PATH}', env=env)

    r = _run('git', 'write-tree', env=env)
    tree = r.stdout.strip()
    TMP_IDX.unlink(missing_ok=True)
    if not tree:
        return False

    cmd = ['git', 'commit-tree', tree, '-m', f'raw {int(time.time())}']
    if parent:
        cmd += ['-p', parent]
    r = _run(*cmd)
    commit = r.stdout.strip()
    if not commit:
        return False

    r = _run('git', 'push', 'origin', f'{commit}:refs/heads/{DATA_BRANCH}')
    return r.returncode == 0


# ── Push loop ─────────────────────────────────────────────────────────────────

def _push_loop():
    TMP_DIR.mkdir(parents=True, exist_ok=True)
    cycle = 0
    while True:
        time.sleep(PUSH_S)
        cycle += 1
        try:
            with _deque_lock:
                if not _raw_deque:
                    continue
                lines = list(_raw_deque)   # O(n) snapshot; bounded at WINDOW_LINES

            # 1. Write to tmpfs (RAM) — fast, no flash, not backed up by Android
            with gzip.open(GZ_FILE, 'wt', compresslevel=1) as f:
                f.write('\n'.join(lines))

            # 2. Push same file to GitHub for remote backup
            ok = _git_push_snapshot(GZ_FILE)

            if cycle % GC_EVERY == 0:
                gc.collect()

            # 3. Prune unreachable git objects created by previous push cycles
            #    Safe because we NEVER fetch data/raw locally — those blobs have
            #    no local ref pointing to them after the push.
            if cycle % GC_PRUNE_EVERY == 0:
                _run('git', 'prune', '--expire=now')

            if cycle % LOG_MEM_EVERY == 0:
                rss = resource.getrusage(resource.RUSAGE_SELF).ru_maxrss
                sz  = GZ_FILE.stat().st_size if GZ_FILE.exists() else 0
                log(f"mem rss={rss}KB  lines={len(lines)}  "
                    f"gz={sz//1024}kB  push={'ok' if ok else 'fail'}  "
                    f"cycle={cycle}", Y)

        except Exception as e:
            log(f"push error: {e}", R)


# ── Code-update check (fetches code branch only, NEVER data/raw) ──────────────

def _check_update():
    try:
        branch = _run('git', 'rev-parse', '--abbrev-ref', 'HEAD').stdout.strip()
        if not branch or branch == 'HEAD':
            return
        _run('git', 'fetch', 'origin', branch)
        local  = _run('git', 'rev-parse', 'HEAD').stdout.strip()
        remote = _run('git', 'rev-parse', f'origin/{branch}').stdout.strip()
        if not remote or remote == local:
            return
        log("update detected — applying and restarting...", Y)
        r = _run('git', 'reset', '--hard', f'origin/{branch}')
        if r.returncode != 0:
            log(f"reset failed: {r.stderr.strip()[:80]} — skipping restart", R)
            return
        new_local = _run('git', 'rev-parse', 'HEAD').stdout.strip()
        if new_local != remote:
            log("still diverged after reset — skipping restart", R)
            return
        os.execv(sys.executable, [sys.executable] + sys.argv)
    except Exception as e:
        log(f"update check error: {e}", R)


# ── WebSocket stream → deque (hot path: no file I/O) ─────────────────────────

async def stream():
    def on_trade(d):
        line = json.dumps(["T",
            int(d['T']),
            int(float(d['p'])*100),
            int(float(d['q'])*10000),
            0 if d.get('m') else 1
        ], separators=(',',':'))
        with _deque_lock:
            _raw_deque.append(line)

    def on_depth(d):
        ts  = int(time.time()*1000)
        bid = [[int(float(p)*100), int(float(q)*10000)] for p,q in d.get('bids',[])]
        ask = [[int(float(p)*100), int(float(q)*10000)] for p,q in d.get('asks',[])]
        line = json.dumps(["D", ts, bid, ask], separators=(',',':'))
        with _deque_lock:
            _raw_deque.append(line)

    log("connecting...", G)
    backoff = 1
    try:
        while True:
            try:
                async with aiohttp.ClientSession() as sess:
                    async with sess.ws_connect(WS_URL, heartbeat=20,
                                               receive_timeout=30) as ws:
                        backoff = 1
                        log(f"connected  deque maxlen={WINDOW_LINES}", G)
                        async for msg in ws:
                            if msg.type == aiohttp.WSMsgType.TEXT:
                                try:
                                    d  = json.loads(msg.data)
                                    st = d.get('stream', '')
                                    da = d.get('data', {})
                                    if 'aggTrade' in st: on_trade(da)
                                    elif '@depth'  in st: on_depth(da)
                                except Exception as e:
                                    log(f"parse err: {e}", R)
                            elif msg.type in (aiohttp.WSMsgType.CLOSED,
                                              aiohttp.WSMsgType.ERROR):
                                break
            except asyncio.CancelledError:
                break
            except Exception as e:
                log(f"error ({e}) retry {backoff}s", R)
                await asyncio.sleep(backoff)
                backoff = min(backoff*2, 60)
    finally:
        pass   # deque lives in RAM — nothing to flush or close


if __name__ == '__main__':
    # Backfill candle history to /tmp/trendrider/ (not into repo).
    # Fetches exactly the gap: 20-min gap → ~20 bars, 20-day gap → ~28 800 bars.
    try:
        import pull_candles
        pull_candles.backfill_local(verbose=True)
    except Exception as e:
        log(f"candle backfill error (non-fatal): {e}", R)

    threading.Thread(target=_push_loop, daemon=True).start()
    loop = asyncio.new_event_loop()
    task = loop.create_task(stream())
    for s in (signal.SIGINT, signal.SIGTERM):
        loop.add_signal_handler(s, task.cancel)
    try:
        loop.run_until_complete(task)
    finally:
        loop.close()
