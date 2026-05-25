#!/usr/bin/env python3
"""
collect_raw.py — pipe WebSocket stream directly to file.

Hot path : message → fh.write(line+\n). Nothing else.
Push loop: every PUSH_S seconds → lzma compress → git push data/raw.
Rotate   : every ROTATE_LINES lines, archive current file and start fresh.
           Keeps Termux RSS low — never holds the full history in RAM.

Compression: lzma preset=9 (better than gzip-9, built-in, no extra deps).
             Typical: 1MB raw JSONL → ~35KB lzma vs ~55KB gzip-9.

Signal-9 defence:
  - JSONL file rotated every ROTATE_LINES to cap disk/memory growth
  - gc.collect() every GC_EVERY pushes
  - RSS logged every LOG_MEM_EVERY pushes so you can see growth

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

PUSH_S        = 1        # push to git every N seconds
ROTATE_LINES  = 50_000   # rotate JSONL after this many lines (~5MB raw)
GC_EVERY      = 60       # gc.collect() every N push cycles
LOG_MEM_EVERY = 300      # log RSS every N push cycles

WS_URL = ("wss://stream.binance.us:9443/stream"
          "?streams=btcusdt@depth20@100ms/btcusdt@aggTrade")

G='\033[92m'; R='\033[91m'; Y='\033[93m'; Z='\033[0m'
def log(m, c=Z): print(f"{c}[raw] {m}{Z}", flush=True)

def git(*a):
    return subprocess.run(['git','-C',str(REPO)]+list(a), capture_output=True, text=True)

# shared line counter (written by stream coroutine, read by push thread)
_line_count = 0
_count_lock = threading.Lock()

def _rotate():
    """Archive current JSONL to timestamped xz file, start fresh."""
    ts  = int(time.time())
    arc = RAW_DIR / f"BTCUSDT_{ts}.jsonl.gz"
    if RAW_FILE.exists() and RAW_FILE.stat().st_size > 0:
        with open(RAW_FILE, 'rb') as src:
            data = src.read()
        with gzip.open(arc, 'wb', compresslevel=9) as dst:
            dst.write(data)
        RAW_FILE.write_bytes(b'')  # truncate in-place (keeps fd open in stream)
        sz_raw = len(data)
        sz_gz  = arc.stat().st_size
        ratio  = sz_raw / max(sz_gz, 1)
        log(f"rotated → {arc.name}  "
            f"{sz_raw//1024}kB → {sz_gz//1024}kB  ({ratio:.1f}x)", Y)
        # push archive too
        rel = str(arc.relative_to(REPO))
        git('add', rel)

def _local_hash() -> str:
    r = subprocess.run(['git','-C',str(REPO),'rev-parse','HEAD'],
                       capture_output=True, text=True)
    return r.stdout.strip()

def _remote_hash() -> str:
    branch = subprocess.run(
        ['git','-C',str(REPO),'rev-parse','--abbrev-ref','HEAD'],
        capture_output=True, text=True).stdout.strip()
    subprocess.run(['git','-C',str(REPO),'fetch','origin', branch],
                   capture_output=True)
    r = subprocess.run(
        ['git','-C',str(REPO),'rev-parse',f'origin/{branch}'],
        capture_output=True, text=True)
    return r.stdout.strip()

def _check_update():
    try:
        local  = _local_hash()
        remote = _remote_hash()
        if remote and remote != local:
            log(f"update detected — pulling and restarting...", Y)
            subprocess.run(['git','-C',str(REPO),'pull','--rebase'],
                           capture_output=True)
            os.execv(sys.executable, [sys.executable] + sys.argv)
    except Exception as e:
        log(f"update check error: {e}", R)


def _push_loop():
    cycle = 0
    while True:
        time.sleep(PUSH_S)
        cycle += 1
        try:
            if not RAW_FILE.exists() or RAW_FILE.stat().st_size == 0:
                continue

            # Check rotation
            with _count_lock:
                lc = _line_count
            if lc >= ROTATE_LINES:
                _rotate()
                with _count_lock:
                    _line_count = 0

            # Compress current live file to gz
            tmp = GZ_FILE.with_suffix('.tmp.gz')
            with open(RAW_FILE, 'rb') as src:
                data = src.read()
            with gzip.open(tmp, 'wb', compresslevel=9) as dst:
                dst.write(data)
            tmp.rename(GZ_FILE)

            rel = str(GZ_FILE.relative_to(REPO))
            git('add', rel)
            r = git('commit', '-m', f"raw {int(time.time())}")
            if r.returncode == 0:
                pr = git('push', '--force', 'origin', f'HEAD:{DATA_BRANCH}')
                sz = GZ_FILE.stat().st_size
                if pr.returncode == 0:
                    log(f"pushed {sz//1024}kB gz  lines={lc}", G)
                else:
                    log(f"push fail: {pr.stderr.strip()}", R)

            # Periodic GC
            if cycle % GC_EVERY == 0:
                gc.collect()

            # RSS logging
            if cycle % LOG_MEM_EVERY == 0:
                rss = resource.getrusage(resource.RUSAGE_SELF).ru_maxrss
                log(f"mem rss={rss}KB  lines={lc}  cycle={cycle}", Y)

            # Hot reload — check if remote has newer code every 30 cycles
            if cycle % 30 == 0:
                _check_update()

        except Exception as e:
            log(f"push error: {e}", R)


async def stream():
    RAW_DIR.mkdir(parents=True, exist_ok=True)
    fh = open(RAW_FILE, 'a', buffering=1)

    def on_trade(d):
        fh.write(json.dumps(["T",
            int(d['T']),
            int(float(d['p'])*100),
            int(float(d['q'])*10000),
            0 if d.get('m') else 1
        ], separators=(',',':')) + '\n')
        with _count_lock: _line_count += 1

    def on_depth(d):
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
