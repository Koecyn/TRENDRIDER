#!/usr/bin/env python3
"""
collect_raw.py — pipe WebSocket stream directly to file.

Hot path : message → fh.write(line+\n). Nothing else.
Push loop: every PUSH_S seconds → stream-compress → git plumbing push to data/raw.
Rotate   : every ROTATE_LINES lines, archive current file and start fresh.

Signal-9 defence:
  - Streaming gzip: never reads whole file into RAM (64KB chunks)
  - Git plumbing: hash-object + commit-tree, never touches code branch or index
  - JSONL rotated every ROTATE_LINES to cap disk growth
  - gc.collect() every GC_EVERY pushes
  - RSS logged every LOG_MEM_EVERY pushes

Trade line: ["T", ts_ms, price_cents, qty_units, side]
Depth line: ["D", ts_ms, [[p_c,q_u],...bids], [[p_c,q_u],...asks]]

Usage: python collect_raw.py
"""

import asyncio
import gc
import gzip
import json
import os
import resource
import shutil
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

REPO          = Path(__file__).resolve().parent
os.chdir(REPO)
RAW_DIR       = REPO / "data" / "raw"
RAW_FILE      = RAW_DIR / "BTCUSDT_LIVE.jsonl"
GZ_FILE       = RAW_DIR / "BTCUSDT_LIVE.jsonl.gz"
DATA_BRANCH   = "data/raw"
TMP_IDX       = REPO / ".git" / "data_push.idx"

PUSH_S        = 1        # push to git every N seconds
ROTATE_LINES  = 50_000   # rotate JSONL after this many lines (~5MB raw)
GC_EVERY      = 60       # gc.collect() every N push cycles
LOG_MEM_EVERY = 300      # log RSS every N push cycles

WS_URL = ("wss://stream.binance.us:9443/stream"
          "?streams=btcusdt@depth20@100ms/btcusdt@aggTrade")

G='\033[92m'; R='\033[91m'; Y='\033[93m'; Z='\033[0m'
def log(m, c=Z): print(f"{c}[raw] {m}{Z}", flush=True)

def _run(*a, env=None):
    return subprocess.run(list(a), capture_output=True, text=True,
                          cwd=str(REPO), env=env)

# shared line counter
_line_count = 0
_count_lock = threading.Lock()


def _stream_compress(src_path, dst_path):
    """Gzip src → dst in 64KB chunks — no full-file read into RAM."""
    tmp = dst_path.with_suffix('.tmp.gz')
    with open(src_path, 'rb') as src, \
         gzip.open(tmp, 'wb', compresslevel=9) as dst:
        shutil.copyfileobj(src, dst, length=65536)
    tmp.rename(dst_path)


def _git_push_file(gz_path) -> bool:
    """
    Push gz_path to DATA_BRANCH using git plumbing only.
    Never touches the working index or code branch.
    """
    rel = str(gz_path.relative_to(REPO))

    # 1. Write blob object
    r = _run('git', 'hash-object', '-w', str(gz_path))
    blob = r.stdout.strip()
    if not blob:
        return False

    # 2. Build a temp index seeded from current data branch
    env = {**os.environ, 'GIT_INDEX_FILE': str(TMP_IDX)}
    parent_r = _run('git', 'rev-parse', f'origin/{DATA_BRANCH}')
    parent = parent_r.stdout.strip()
    if parent:
        _run('git', 'read-tree', f'origin/{DATA_BRANCH}', env=env)

    # 3. Update only our file in the temp index
    _run('git', 'update-index', '--add',
         '--cacheinfo', f'100644,{blob},{rel}', env=env)

    # 4. Write tree from temp index
    r = _run('git', 'write-tree', env=env)
    tree = r.stdout.strip()
    TMP_IDX.unlink(missing_ok=True)
    if not tree:
        return False

    # 5. Create commit object
    cmd = ['git', 'commit-tree', tree, '-m', f'raw {int(time.time())}']
    if parent:
        cmd += ['-p', parent]
    r = _run(*cmd)
    commit = r.stdout.strip()
    if not commit:
        return False

    # 6. Push commit directly to data branch ref
    r = _run('git', 'push', 'origin', f'{commit}:refs/heads/{DATA_BRANCH}')
    return r.returncode == 0


def _rotate():
    """Archive current JSONL to timestamped gz, start fresh."""
    ts  = int(time.time())
    arc = RAW_DIR / f"BTCUSDT_{ts}.jsonl.gz"
    if RAW_FILE.exists() and RAW_FILE.stat().st_size > 0:
        sz_raw = RAW_FILE.stat().st_size
        _stream_compress(RAW_FILE, arc)
        RAW_FILE.write_bytes(b'')
        sz_gz = arc.stat().st_size
        ratio = sz_raw / max(sz_gz, 1)
        log(f"rotated → {arc.name}  "
            f"{sz_raw//1024}kB → {sz_gz//1024}kB  ({ratio:.1f}x)", Y)
        _git_push_file(arc)


def _check_update():
    try:
        branch = _run('git', 'rev-parse', '--abbrev-ref', 'HEAD').stdout.strip()
        _run('git', 'fetch', 'origin', branch)
        local  = _run('git', 'rev-parse', 'HEAD').stdout.strip()
        remote = _run('git', 'rev-parse', f'origin/{branch}').stdout.strip()
        if remote and remote != local:
            log("update detected — pulling and restarting...", Y)
            _run('git', 'pull', '--rebase')
            os.execv(sys.executable, [sys.executable] + sys.argv)
    except Exception as e:
        log(f"update check error: {e}", R)


def _push_loop():
    global _line_count
    cycle = 0
    while True:
        time.sleep(PUSH_S)
        cycle += 1
        try:
            if not RAW_FILE.exists() or RAW_FILE.stat().st_size == 0:
                continue

            with _count_lock:
                lc = _line_count
            if lc >= ROTATE_LINES:
                _rotate()
                with _count_lock:
                    _line_count = 0

            _stream_compress(RAW_FILE, GZ_FILE)

            ok = _git_push_file(GZ_FILE)
            sz = GZ_FILE.stat().st_size
            if ok:
                log(f"pushed {sz//1024}kB gz  lines={lc}", G)
            else:
                log(f"push fail", R)

            if cycle % GC_EVERY == 0:
                gc.collect()

            if cycle % LOG_MEM_EVERY == 0:
                rss = resource.getrusage(resource.RUSAGE_SELF).ru_maxrss
                log(f"mem rss={rss}KB  lines={lc}  cycle={cycle}", Y)

            if cycle % 5 == 0:
                _check_update()

        except Exception as e:
            log(f"push error: {e}", R)


async def stream():
    RAW_DIR.mkdir(parents=True, exist_ok=True)
    fh = open(RAW_FILE, 'a', buffering=1)

    def on_trade(d):
        global _line_count
        fh.write(json.dumps(["T",
            int(d['T']),
            int(float(d['p'])*100),
            int(float(d['q'])*10000),
            0 if d.get('m') else 1
        ], separators=(',',':')) + '\n')
        with _count_lock: _line_count += 1

    def on_depth(d):
        global _line_count
        ts  = int(time.time()*1000)
        bid = [[int(float(p)*100),int(float(q)*10000)] for p,q in d.get('bids',[])]
        ask = [[int(float(p)*100),int(float(q)*10000)] for p,q in d.get('asks',[])]
        fh.write(json.dumps(["D",ts,bid,ask], separators=(',',':')) + '\n')
        with _count_lock: _line_count += 1

    log("connecting...", G)
    backoff = 1
    try:
        while True:
            try:
                async with aiohttp.ClientSession() as sess:
                    async with sess.ws_connect(WS_URL, heartbeat=20,
                                               receive_timeout=30) as ws:
                        backoff = 1
                        log("connected — writing direct to file", G)
                        async for msg in ws:
                            if msg.type == aiohttp.WSMsgType.TEXT:
                                try:
                                    d  = json.loads(msg.data)
                                    st = d.get('stream','')
                                    da = d.get('data',{})
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
                if not fh.closed:
                    log(f"error ({e}) retry {backoff}s", R)
                await asyncio.sleep(backoff)
                backoff = min(backoff*2, 60)
    finally:
        fh.close()


if __name__ == '__main__':
    threading.Thread(target=_push_loop, daemon=True).start()
    loop = asyncio.new_event_loop()
    task = loop.create_task(stream())
    for s in (signal.SIGINT, signal.SIGTERM):
        loop.add_signal_handler(s, task.cancel)
    try:
        loop.run_until_complete(task)
    finally:
        loop.close()
