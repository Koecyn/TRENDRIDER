#!/usr/bin/env python3
"""
collect_raw.py — live raw stream to git.

Every aggTrade and every depth snapshot written to file immediately as
it arrives from the exchange. Git push after every write.

Trade line:  ["T", delta_ms, price_cents, qty_units, side]
Depth line:  ["D", ts_ms, [[p_cents,q_units],...bids], [[p_cents,q_units],...asks]]

File: data/raw/BTCUSDC_LIVE.jsonl.gz  (rolling, overwritten each push)
Push: origin data/live branch

Usage: python collect_raw.py
"""

import asyncio
import gzip
import json
import signal
import subprocess
import sys
import time
from pathlib import Path

try:
    import aiohttp
except ImportError:
    print("ERROR: pip install aiohttp"); sys.exit(1)

REPO        = Path(__file__).resolve().parent
OUT_FILE    = REPO / "data" / "raw" / "BTCUSDC_LIVE.jsonl.gz"
DATA_BRANCH = "data/live"
WS_URL      = ("wss://stream.binance.us:9443/stream"
               "?streams=btcusdc@depth20@100ms/btcusdc@aggTrade")

G='\033[92m'; R='\033[91m'; Y='\033[93m'; Z='\033[0m'
def log(m,c=Z): print(f"{c}[raw] {m}{Z}", flush=True)

def git(*a):
    return subprocess.run(['git','-C',str(REPO)]+list(a),
                          capture_output=True, text=True)

def push(lines: list):
    OUT_FILE.parent.mkdir(parents=True, exist_ok=True)
    tmp = OUT_FILE.with_suffix('.tmp.gz')
    with gzip.open(tmp, 'wt', compresslevel=9) as f:
        for line in lines:
            f.write(line + '\n')
    tmp.rename(OUT_FILE)
    rel = str(OUT_FILE.relative_to(REPO))
    git('add', rel)
    r = git('commit', '-m', f"raw {int(time.time()*1000)} n={len(lines)}")
    if r.returncode == 0:
        git('push', 'origin', f'HEAD:{DATA_BRANCH}')


class Streamer:
    def __init__(self):
        self._lines   = []
        self._running = True

    def on_trade(self, d):
        rec = json.dumps(["T",
            int(d['T']),
            int(float(d['p'])*100),
            int(float(d['q'])*10000),
            0 if d.get('m') else 1
        ], separators=(',',':'))
        self._lines.append(rec)
        push(self._lines)

    def on_depth(self, d):
        ts  = int(time.time()*1000)
        bid = [[int(float(p)*100), int(float(q)*10000)] for p,q in d.get('bids',[])]
        ask = [[int(float(p)*100), int(float(q)*10000)] for p,q in d.get('asks',[])]
        rec = json.dumps(["D", ts, bid, ask], separators=(',',':'))
        self._lines.append(rec)
        push(self._lines)

    def stop(self): self._running = False

    async def run(self):
        log("connecting...", Y)
        backoff = 1
        while self._running:
            try:
                async with aiohttp.ClientSession() as sess:
                    async with sess.ws_connect(WS_URL, heartbeat=20,
                                               receive_timeout=30) as ws:
                        backoff = 1
                        log("connected", G)
                        async for msg in ws:
                            if not self._running: break
                            if msg.type == aiohttp.WSMsgType.TEXT:
                                try:
                                    d  = json.loads(msg.data)
                                    st = d.get('stream','')
                                    da = d.get('data',{})
                                    if 'aggTrade' in st: self.on_trade(da)
                                    elif '@depth'  in st: self.on_depth(da)
                                except Exception as e:
                                    log(f"parse: {e}", R)
                            elif msg.type in (aiohttp.WSMsgType.CLOSED,
                                              aiohttp.WSMsgType.ERROR):
                                break
            except asyncio.CancelledError: break
            except Exception as e:
                if not self._running: break
                log(f"error ({e}) retry {backoff}s", R)
                await asyncio.sleep(backoff)
                backoff = min(backoff*2, 60)
        log("stopped", Y)


async def _main():
    s    = Streamer()
    loop = asyncio.get_event_loop()
    for sig in (signal.SIGINT, signal.SIGTERM):
        loop.add_signal_handler(sig, s.stop)
    await s.run()

if __name__ == '__main__':
    asyncio.run(_main())
