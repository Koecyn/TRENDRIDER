#!/usr/bin/env python3
"""
collect_raw.py — raw market data collector for BTCUSDC.

Connects to Binance.US WebSocket and saves every closed 1m bar plus a
full L20 order book snapshot at bar close.  Data is written to gzip-
compressed JSON files under data/raw/ so the physics engine can be
replayed later against a large corpus of real order-book data.

Output files:
  data/raw/BTCUSDC_1m_YYYYMMDD.json.gz   — one file per UTC day

Each file is a JSON object:
  {
    "symbol":    "BTCUSDC",
    "interval":  "1m",
    "date":      "YYYY-MM-DD",
    "bars": [
      {
        "ts":        <open_time_ms>,
        "open":      <float>,
        "high":      <float>,
        "low":       <float>,
        "close":     <float>,
        "volume":    <float>,
        "taker_buy": <float>,
        "book": {
          "ts":   <close_time_ms>,
          "bids": [[price, qty], ...],   # L20
          "asks": [[price, qty], ...]
        }
      },
      ...
    ]
  }

The "book" snapshot is taken from the most recent depth20 push before
bar close — real order book depth at the moment the candle sealed.

Usage (Termux foreground):
  python collect_raw.py

Usage (background, survives session close):
  nohup python collect_raw.py >> collect_raw.log 2>&1 &

Replay:
  from collect_raw import load_day, iter_replay
  for bar, bids, asks in iter_replay("2026-05-22"):
      # bar = dict with same keys physics_live on_bar() expects
      # bids/asks = L20 at close, same format as live engine
"""

import asyncio
import gzip
import json
import os
import re
import signal
import sys
import time
from collections import deque
from datetime import datetime, timezone, timedelta
from pathlib import Path

try:
    import aiohttp
except ImportError:
    print("ERROR: pip install aiohttp")
    sys.exit(1)

# ── Config ────────────────────────────────────────────────────────────────────

SYMBOL      = "BTCUSDC"
INTERVAL    = "1m"
OUT_DIR     = Path(__file__).resolve().parent / "data" / "raw"
LOG_F       = Path(__file__).resolve().parent / "collect_raw.log"

WS_URL = ("wss://stream.binance.us:9443/stream"
          "?streams=btcusdc@kline_1m/btcusdc@depth20@100ms")

# Flush a checkpoint to disk every N closed bars (in addition to day rollover)
FLUSH_EVERY = 30

# ── Colour helpers ────────────────────────────────────────────────────────────

G='\033[92m'; R='\033[91m'; Y='\033[93m'; C='\033[96m'; B='\033[1m'; Z='\033[0m'


def ts_str():
    return datetime.now().strftime('%H:%M:%S')


def log(msg, col=Z):
    line = f"{col}[collect {ts_str()}] {msg}{Z}"
    print(line, flush=True)
    try:
        with open(LOG_F, 'a') as f:
            f.write(re.sub(r'\033\[[0-9;]*m', '', line) + '\n')
    except Exception:
        pass


# ── File helpers ──────────────────────────────────────────────────────────────

def _day_path(date_str: str) -> Path:
    return OUT_DIR / f"{SYMBOL}_{INTERVAL}_{date_str}.json.gz"


def _load_existing(date_str: str) -> list:
    """Load bars already collected today (resume after crash)."""
    p = _day_path(date_str)
    if not p.exists():
        return []
    try:
        with gzip.open(p, 'rt', encoding='utf-8') as f:
            data = json.load(f)
        bars = data.get('bars', [])
        log(f"  Resumed: {len(bars)} bars already in {p.name}", Y)
        return bars
    except Exception as e:
        log(f"  Resume failed ({e}) — starting fresh", Y)
        return []


def _flush(date_str: str, bars: list):
    """Write bars to today's gzip file (atomic via temp file)."""
    OUT_DIR.mkdir(parents=True, exist_ok=True)
    p    = _day_path(date_str)
    tmp  = p.with_suffix('.tmp.gz')
    payload = {
        'symbol':   SYMBOL,
        'interval': INTERVAL,
        'date':     date_str,
        'bars':     bars,
    }
    with gzip.open(tmp, 'wt', encoding='utf-8', compresslevel=6) as f:
        json.dump(payload, f, separators=(',', ':'))
    tmp.rename(p)


# ── Collector ─────────────────────────────────────────────────────────────────

class Collector:

    def __init__(self):
        self._bids:      list  = []
        self._asks:      list  = []
        self._book_ts:   int   = 0

        self._running:   bool  = True
        self._today:     str   = self._utc_date()
        self._bars:      list  = _load_existing(self._today)
        self._bar_count: int   = 0  # closed bars since last flush

    @staticmethod
    def _utc_date() -> str:
        return datetime.now(timezone.utc).strftime('%Y-%m-%d')

    def _handle_depth(self, data: dict):
        self._bids    = [[float(p), float(q)] for p, q in data.get('bids', [])]
        self._asks    = [[float(p), float(q)] for p, q in data.get('asks', [])]
        self._book_ts = int(time.time() * 1000)

    def _handle_kline(self, k: dict):
        if not k.get('x', False):
            return   # only closed bars

        bar = {
            'ts':        int(k['t']),
            'open':      float(k['o']),
            'high':      float(k['h']),
            'low':       float(k['l']),
            'close':     float(k['c']),
            'volume':    float(k['v']),
            'taker_buy': float(k['V']),
            'book': {
                'ts':   self._book_ts,
                'bids': list(self._bids),
                'asks': list(self._asks),
            },
        }

        # Day rollover
        day = self._utc_date()
        if day != self._today:
            log(f"Day rollover: {self._today} → {day}  "
                f"({len(self._bars)} bars saved)", G)
            _flush(self._today, self._bars)
            self._today = day
            self._bars  = []

        self._bars.append(bar)
        self._bar_count += 1

        close_dt = datetime.fromtimestamp(int(k['T']) / 1000)
        log(f"Bar {close_dt.strftime('%H:%M')}  "
            f"close={bar['close']:.2f}  vol={bar['volume']:.3f}  "
            f"book_levels={len(self._bids)}/{len(self._asks)}  "
            f"total={len(self._bars)}", C)

        if self._bar_count >= FLUSH_EVERY:
            _flush(self._today, self._bars)
            sz = _day_path(self._today).stat().st_size / 1024
            log(f"  Checkpoint: {len(self._bars)} bars → {sz:.1f} kB", G)
            self._bar_count = 0

    def stop(self):
        self._running = False

    async def run(self):
        log(f"Collector starting — {SYMBOL} {INTERVAL}", B)
        log(f"Output: {OUT_DIR}", C)
        OUT_DIR.mkdir(parents=True, exist_ok=True)

        backoff = 1
        while self._running:
            try:
                async with aiohttp.ClientSession() as sess:
                    async with sess.ws_connect(WS_URL, heartbeat=20,
                                               receive_timeout=90) as ws:
                        backoff = 1
                        log("Connected to Binance.US WebSocket", G)
                        async for msg in ws:
                            if not self._running:
                                break
                            if msg.type == aiohttp.WSMsgType.TEXT:
                                try:
                                    d      = json.loads(msg.data)
                                    stream = d.get('stream', '')
                                    data   = d.get('data', {})
                                    if '@kline' in stream:
                                        self._handle_kline(data.get('k', {}))
                                    elif '@depth' in stream:
                                        self._handle_depth(data)
                                except Exception as e:
                                    log(f"Parse error: {e}", R)
                            elif msg.type in (aiohttp.WSMsgType.CLOSED,
                                              aiohttp.WSMsgType.ERROR):
                                log("WS closed — reconnecting", Y)
                                break
            except asyncio.CancelledError:
                break
            except Exception as e:
                if not self._running:
                    break
                log(f"Connection error ({e}) — retry in {backoff}s", R)
                await asyncio.sleep(backoff)
                backoff = min(backoff * 2, 60)

        # Final flush on clean exit
        if self._bars:
            _flush(self._today, self._bars)
            sz = _day_path(self._today).stat().st_size / 1024
            log(f"Final flush: {len(self._bars)} bars → {sz:.1f} kB", G)
        log("Collector stopped", Y)


# ── Replay helpers (import from other scripts) ────────────────────────────────

def load_day(date_str: str) -> list:
    """
    Load a single day's bars from disk.
    Returns list of bar dicts (same format as physics_live window).
    Includes 'bids' and 'asks' fields extracted from bar['book'].
    """
    p = _day_path(date_str)
    if not p.exists():
        raise FileNotFoundError(f"{p} not found")
    with gzip.open(p, 'rt', encoding='utf-8') as f:
        data = json.load(f)
    return data.get('bars', [])


def load_range(start: str, end: str) -> list:
    """
    Load bars across a date range (inclusive).
    start / end: 'YYYY-MM-DD'
    Returns flat list sorted by timestamp.
    """
    d     = datetime.strptime(start, '%Y-%m-%d')
    end_d = datetime.strptime(end,   '%Y-%m-%d')
    bars  = []
    while d <= end_d:
        ds = d.strftime('%Y-%m-%d')
        try:
            bars.extend(load_day(ds))
        except FileNotFoundError:
            pass
        d += timedelta(days=1)
    bars.sort(key=lambda b: b['ts'])
    return bars


def iter_replay(date_str: str = None, start: str = None, end: str = None):
    """
    Generator — yields (bar_dict, bids, asks) tuples in bar-close order.

    Usage:
        for bar, bids, asks in iter_replay('2026-05-22'):
            sig = fusion.run(..., bids=bids, asks=asks)

    bids / asks are lists of [price, qty] (same as live engine).
    """
    if date_str:
        bars = load_day(date_str)
    elif start and end:
        bars = load_range(start, end)
    else:
        raise ValueError("pass date_str or start+end")

    for bar in bars:
        book = bar.get('book', {})
        bids = [(float(p), float(q)) for p, q in book.get('bids', [])]
        asks = [(float(p), float(q)) for p, q in book.get('asks', [])]
        yield bar, bids, asks


def list_days() -> list:
    """Return sorted list of collected date strings."""
    if not OUT_DIR.exists():
        return []
    days = []
    for p in sorted(OUT_DIR.glob(f"{SYMBOL}_{INTERVAL}_*.json.gz")):
        m = re.search(r'(\d{4}-\d{2}-\d{2})', p.name)
        if m:
            days.append(m.group(1))
    return days


def summary():
    """Print a summary of all collected data."""
    days = list_days()
    if not days:
        print("No data collected yet.")
        return
    total = 0
    total_sz = 0
    print(f"\nCollected data — {OUT_DIR}\n{'─'*50}")
    for ds in days:
        p = _day_path(ds)
        sz = p.stat().st_size
        try:
            bars = load_day(ds)
            n = len(bars)
        except Exception:
            n = 0
        total     += n
        total_sz  += sz
        print(f"  {ds}  {n:5d} bars  {sz/1024:7.1f} kB")
    print(f"{'─'*50}")
    print(f"  Total    {total:5d} bars  {total_sz/1024:7.1f} kB\n")


# ── Entry point ───────────────────────────────────────────────────────────────

async def _main():
    collector = Collector()
    loop = asyncio.get_event_loop()
    for sig in (signal.SIGINT, signal.SIGTERM):
        loop.add_signal_handler(sig, collector.stop)
    await collector.run()


if __name__ == '__main__':
    if len(sys.argv) > 1 and sys.argv[1] == 'summary':
        summary()
    else:
        asyncio.run(_main())
