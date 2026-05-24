#!/usr/bin/env python3
"""
collect_raw.py — pipe WebSocket stream directly to file.

Hot path: message arrives → write line → flush. Nothing else.
Background thread: every PUSH_S seconds → gzip-9 → git push to data/live.

Trade line: ["T", ts_ms, price_cents, qty_units, side]
Depth line: ["D", ts_ms, [[p_c,q_u],...bids], [[p_c,q_u],...asks]]

Usage: python collect_raw.py
"""

import asyncio
import gzip
import json
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

REPO        = Path(__file__).resolve().parent
RAW_DIR     = REPO / "data" / "raw"
RAW_FILE    = RAW_DIR / "BTCUSDC_LIVE.jsonl"
GZ_FILE     = RAW_DIR / "BTCUSDC_LIVE.jsonl.gz"
DATA_BRANCH = "data/raw"
PUSH_S      = 10

WS_URL = ("wss://stream.binance.us:9443/stream"
          "?streams=btcusdc@depth20@100ms/btcusdc@aggTrade")

G='\033[92m'; R='\033[91m'; Z='\033[0m'
def log(m, c=Z): print(f"{c}[raw] {m}{Z}", flush=True)

def git(*a):
    subprocess.run(['git','-C',str(REPO)]+list(a), capture_output=True)

def _push_loop():
    """Background: compress latest JSONL and push to git every PUSH_S seconds."""
    while True:
        time.sleep(PUSH_S)
        try:
            if not RAW_FILE.exists() or RAW_FILE.stat().st_size == 0:
                continue
            tmp = GZ_FILE.with_suffix('.tmp.gz')
            with open(RAW_FILE, 'rb') as src, \
                 gzip.open(tmp, 'wb', compresslevel=9) as dst:
                dst.write(src.read())
            tmp.rename(GZ_FILE)
            rel = str(GZ_FILE.relative_to(REPO))
            git('add', rel)
            r = subprocess.run(
                ['git','-C',str(REPO),'commit','-m',
                 f"raw {int(time.time())}"],
                capture_output=True, text=True)
            if r.returncode == 0:
                pr = subprocess.run(
                    ['git','-C',str(REPO),'push','--force','origin',
                     f'HEAD:{DATA_BRANCH}'],
                    capture_output=True, text=True)
                sz = GZ_FILE.stat().st_size
                if pr.returncode == 0:
                    log(f"pushed {sz//1024}kB", G)
                else:
                    log(f"push fail: {pr.stderr.strip()}", R)
        except Exception as e:
            log(f"push error: {e}", R)


async def stream():
    RAW_DIR.mkdir(parents=True, exist_ok=True)
    fh = open(RAW_FILE, 'a', buffering=1)   # line-buffered

    def on_trade(d):
        line = json.dumps(["T",
            int(d['T']),
            int(float(d['p'])*100),
            int(float(d['q'])*10000),
            0 if d.get('m') else 1
        ], separators=(',',':'))
        fh.write(line + '\n')

    def on_depth(d):
        ts  = int(time.time()*1000)
        bid = [[int(float(p)*100),int(float(q)*10000)] for p,q in d.get('bids',[])]
        ask = [[int(float(p)*100),int(float(q)*10000)] for p,q in d.get('asks',[])]
        fh.write(json.dumps(["D",ts,bid,ask], separators=(',',':')) + '\n')

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
                                except Exception: pass
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
