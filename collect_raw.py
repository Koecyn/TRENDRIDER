#!/usr/bin/env python3
"""
collect_raw.py — WebSocket → deque → pipeline to wave_scan + GitHub backup.

Pipeline:
  WebSocket → deque(maxlen=15000) → ~/.trendrider/BTCUSDT_LIVE.jsonl.gz
                                  → git push queue → GitHub data/raw

Data dir is ~/.trendrider/ — always writable, outside the repo, not git-tracked.
Git push is queued (non-blocking): write never waits on network.

Trade line : ["T", ts_ms, price_cents, qty_units, side]
Depth line : ["D", ts_ms, [[p_c,q_u],...bids], [[p_c,q_u],...asks]]
"""

import asyncio, collections, gc, gzip, json, os, queue, resource
import signal, subprocess, sys, threading, time
from pathlib import Path

try:
    import aiohttp
except ImportError:
    print("ERROR: pip install aiohttp"); sys.exit(1)

REPO      = Path(__file__).resolve().parent
DATA_DIR  = Path.home() / '.trendrider'          # pipeline data dir — always writable
GZ_FILE   = DATA_DIR / 'BTCUSDT_LIVE.jsonl.gz'  # wave_scan reads this
TMP_IDX   = REPO / '.git' / 'data_push.idx'

DATA_BRANCH   = 'data/raw'
GIT_TREE_PATH = 'data/raw/BTCUSDT_LIVE.jsonl.gz'

WS_URL = ('wss://stream.binance.us:9443/stream'
          '?streams=btcusdt@depth20@100ms/btcusdt@aggTrade')

WINDOW_LINES   = 15_000   # bounded deque — ~125 min at 2 records/sec
PUSH_S         = 2        # write snapshot every N seconds
GC_EVERY       = 60
LOG_MEM_EVERY  = 300
GC_PRUNE_EVERY = 150      # git prune every N cycles to sweep loose objects

_raw_deque  = collections.deque(maxlen=WINDOW_LINES)
_deque_lock = threading.Lock()
_push_queue = queue.Queue(maxsize=1)   # non-blocking git push pipeline

G='\033[92m'; R='\033[91m'; Y='\033[93m'; Z='\033[0m'
def log(m, c=Z): print(f'{c}[raw] {m}{Z}', flush=True)

def _run(*a, env=None):
    return subprocess.run(list(a), capture_output=True, text=True,
                          cwd=str(REPO), env=env)


# ── Git push thread — reads from queue, never blocks the write loop ───────────

def _git_push_worker():
    """Dedicated thread: drains _push_queue and pushes to GitHub."""
    prune_cycle = 0
    while True:
        gz_path = _push_queue.get()   # blocks until a snapshot is queued
        if gz_path is None:
            break
        prune_cycle += 1
        try:
            r = _run('git', 'hash-object', '-w', str(gz_path))
            blob = r.stdout.strip()
            if not blob: continue

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
            if not tree: continue

            cmd = ['git', 'commit-tree', tree, '-m', f'raw {int(time.time())}']
            if parent: cmd += ['-p', parent]
            r = _run(*cmd)
            commit = r.stdout.strip()
            if not commit: continue

            _run('git', 'push', 'origin', f'{commit}:refs/heads/{DATA_BRANCH}')

            if prune_cycle % GC_PRUNE_EVERY == 0:
                _run('git', 'prune', '--expire=now')

        except Exception as e:
            log(f'git push error: {e}', R)


# ── Write loop — snapshot deque to disk, queue git push ──────────────────────

def _push_loop():
    DATA_DIR.mkdir(parents=True, exist_ok=True)
    log(f'pipeline → {DATA_DIR}', G)
    cycle = 0
    while True:
        time.sleep(PUSH_S)
        cycle += 1
        try:
            with _deque_lock:
                if not _raw_deque: continue
                lines = list(_raw_deque)

            with gzip.open(GZ_FILE, 'wt', compresslevel=1) as f:
                f.write('\n'.join(lines))

            # Queue for git push — drop if queue is full (push still in progress)
            try:
                _push_queue.put_nowait(GZ_FILE)
            except queue.Full:
                pass

            if cycle % GC_EVERY == 0:
                gc.collect()

            if cycle % LOG_MEM_EVERY == 0:
                rss = resource.getrusage(resource.RUSAGE_SELF).ru_maxrss
                sz  = GZ_FILE.stat().st_size if GZ_FILE.exists() else 0
                log(f'mem={rss}KB  lines={len(lines)}  gz={sz//1024}kB  '
                    f'cycle={cycle}', Y)
        except Exception as e:
            log(f'write error: {e}', R)


# ── Code-update check ─────────────────────────────────────────────────────────

def _check_update():
    try:
        branch = _run('git', 'rev-parse', '--abbrev-ref', 'HEAD').stdout.strip()
        if not branch or branch == 'HEAD': return
        _run('git', 'fetch', 'origin', branch)
        local  = _run('git', 'rev-parse', 'HEAD').stdout.strip()
        remote = _run('git', 'rev-parse', f'origin/{branch}').stdout.strip()
        if not remote or remote == local: return
        log('update detected — restarting...', Y)
        r = _run('git', 'reset', '--hard', f'origin/{branch}')
        if r.returncode != 0: return
        os.execv(sys.executable, [sys.executable] + sys.argv)
    except Exception as e:
        log(f'update check error: {e}', R)


# ── WebSocket → deque (hot path — zero I/O) ───────────────────────────────────

async def stream():
    def on_trade(d):
        with _deque_lock:
            _raw_deque.append(json.dumps(
                ['T', int(d['T']), int(float(d['p'])*100),
                 int(float(d['q'])*10000), 0 if d.get('m') else 1],
                separators=(',',':')))

    def on_depth(d):
        ts  = int(time.time()*1000)
        bid = [[int(float(p)*100), int(float(q)*10000)] for p,q in d.get('bids',[])]
        ask = [[int(float(p)*100), int(float(q)*10000)] for p,q in d.get('asks',[])]
        with _deque_lock:
            _raw_deque.append(json.dumps(['D', ts, bid, ask], separators=(',',':')))

    log('connecting...', G)
    backoff = 1
    try:
        while True:
            try:
                async with aiohttp.ClientSession() as sess:
                    async with sess.ws_connect(WS_URL, heartbeat=20,
                                               receive_timeout=30) as ws:
                        backoff = 1
                        log(f'connected  deque={WINDOW_LINES}', G)
                        async for msg in ws:
                            if msg.type == aiohttp.WSMsgType.TEXT:
                                try:
                                    d  = json.loads(msg.data)
                                    st = d.get('stream', '')
                                    da = d.get('data', {})
                                    if 'aggTrade' in st: on_trade(da)
                                    elif '@depth'  in st: on_depth(da)
                                except Exception as e:
                                    log(f'parse: {e}', R)
                            elif msg.type in (aiohttp.WSMsgType.CLOSED,
                                              aiohttp.WSMsgType.ERROR):
                                break
            except asyncio.CancelledError:
                break
            except Exception as e:
                log(f'error ({e}) retry {backoff}s', R)
                await asyncio.sleep(backoff)
                backoff = min(backoff*2, 60)
    finally:
        _push_queue.put(None)   # signal git worker to exit


if __name__ == '__main__':
    try:
        import pull_candles
        pull_candles.backfill_local(verbose=True)
    except Exception as e:
        log(f'candle backfill (non-fatal): {e}', R)

    threading.Thread(target=_push_loop,       daemon=True).start()
    threading.Thread(target=_git_push_worker, daemon=True).start()

    loop = asyncio.new_event_loop()
    task = loop.create_task(stream())
    for s in (signal.SIGINT, signal.SIGTERM):
        loop.add_signal_handler(s, task.cancel)
    try:
        loop.run_until_complete(task)
    finally:
        loop.close()
