#!/usr/bin/env python3
"""
collect_raw.py — raw market data collector for BTCUSDC.

Collects every aggTrade and every depth20 snapshot from Binance.US,
compresses at gzip level 9, pushes to the data/live git branch so the
remote analysis environment can read it without touching the main branch.

Per-bar data:
  - OHLCV from kline stream (authoritative, no reconstruction needed)
  - Full aggTrade list for the bar (compact integer encoding)
  - L20 order book snapshot at bar close

Trade record format (compact integers, gzip compresses 10x+ at level 9):
  [ts_delta_ms, price_cents, qty_units, side]
  ts_delta_ms = ms since bar open (fits uint32)
  price_cents = int(price * 100)
  qty_units   = int(qty * 10000)
  side        = 1 buy / 0 sell

OB snapshot: [[price_cents_int, qty_units_int], ...] for bids and asks.
Integer encoding + gzip-9 → roughly 8-12x smaller than float JSON.

Output: data/raw/BTCUSDC_1m_YYYYMMDD.json.gz  (one file per UTC day)
Push:   after every closed bar → data/live branch (same push used by physics_live)

Usage:
  python collect_raw.py          # collect + push
  python collect_raw.py summary  # show what's been collected
"""

import asyncio
import gzip
import json
import os
import re
import signal
import subprocess
import sys
import time
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
REPO        = Path(__file__).resolve().parent
OUT_DIR     = REPO / "data" / "raw"
DATA_BRANCH = "data/live"
COMPRESS    = 9   # gzip max — drops JSON from ~400 kB/day to ~35 kB

WS_URL = ("wss://stream.binance.us:9443/stream"
          "?streams=btcusdc@kline_1m/btcusdc@depth20@100ms/btcusdc@aggTrade")

# ── Helpers ───────────────────────────────────────────────────────────────────

G='\033[92m'; R='\033[91m'; Y='\033[93m'; C='\033[96m'; B='\033[1m'; Z='\033[0m'

def ts_str():
    return datetime.now().strftime('%H:%M:%S')

def log(msg, col=Z):
    print(f"{col}[raw {ts_str()}] {msg}{Z}", flush=True)

def git(*args):
    return subprocess.run(['git', '-C', str(REPO)] + list(args),
                          capture_output=True, text=True)

def _day_path(date_str: str) -> Path:
    return OUT_DIR / f"{SYMBOL}_{INTERVAL}_{date_str}.json.gz"

def _load_existing(date_str: str) -> list:
    p = _day_path(date_str)
    if not p.exists():
        return []
    try:
        with gzip.open(p, 'rt') as f:
            return json.load(f).get('bars', [])
    except Exception:
        return []

def _flush(date_str: str, bars: list) -> Path:
    OUT_DIR.mkdir(parents=True, exist_ok=True)
    p   = _day_path(date_str)
    tmp = p.with_suffix('.tmp.gz')
    with gzip.open(tmp, 'wt', compresslevel=COMPRESS) as f:
        json.dump({'symbol': SYMBOL, 'interval': INTERVAL,
                   'date': date_str, 'bars': bars},
                  f, separators=(',', ':'))
    tmp.rename(p)
    return p

def _git_push(path: Path, bar_n: int, close_price: float):
    rel = str(path.relative_to(REPO))
    git('add', rel)
    r = git('commit', '-m', f"raw {ts_str()} bar={bar_n} c={close_price:.2f}")
    if r.returncode == 0:
        pr = git('push', 'origin', f'HEAD:{DATA_BRANCH}')
        if pr.returncode == 0:
            sz = path.stat().st_size / 1024
            log(f"  pushed {path.name}  {sz:.1f} kB  ({bar_n} bars)", G)
        else:
            log(f"  push failed: {pr.stderr.strip()}", R)
    else:
        log(f"  commit failed: {r.stderr.strip()}", R)


# ── Collector ─────────────────────────────────────────────────────────────────

class Collector:

    def __init__(self):
        self._bids:       list  = []
        self._asks:       list  = []
        self._book_ts:    int   = 0
        self._bar_trades: list  = []
        self._bar_open_ms: int  = 0

        self._running:    bool  = True
        self._today:      str   = self._utc_date()
        self._bars:       list  = _load_existing(self._today)
        if self._bars:
            log(f"Resumed: {len(self._bars)} bars already in {self._today}", Y)

    @staticmethod
    def _utc_date() -> str:
        return datetime.now(timezone.utc).strftime('%Y-%m-%d')

    def _handle_depth(self, data: dict):
        # Store as compact integers — price*100, qty*10000
        self._bids    = [[int(float(p)*100), int(float(q)*10000)]
                         for p, q in data.get('bids', [])]
        self._asks    = [[int(float(p)*100), int(float(q)*10000)]
                         for p, q in data.get('asks', [])]
        self._book_ts = int(time.time() * 1000)

    def _handle_trade(self, data: dict):
        ts_ms = int(data['T'])
        delta = ts_ms - self._bar_open_ms if self._bar_open_ms else 0
        # [ts_delta_ms, price_cents, qty_units, side(1=buy/0=sell)]
        self._bar_trades.append([
            max(0, delta),
            int(float(data['p']) * 100),
            int(float(data['q']) * 10000),
            0 if data.get('m') else 1,
        ])

    def _handle_kline(self, k: dict):
        ts_open = int(k['t'])
        if self._bar_open_ms == 0:
            self._bar_open_ms = ts_open
        if ts_open != self._bar_open_ms:
            self._bar_open_ms = ts_open

        if not k.get('x', False):
            return   # only act on closed bars

        trades   = list(self._bar_trades)
        self._bar_trades = []
        close_px = float(k['c'])

        buy_vol  = sum(t[2] for t in trades if t[3] == 1) / 10000.0
        sell_vol = sum(t[2] for t in trades if t[3] == 0) / 10000.0

        bar = {
            'ts':        int(k['t']),
            'open':      float(k['o']),
            'high':      float(k['h']),
            'low':       float(k['l']),
            'close':     close_px,
            'volume':    float(k['v']),
            'taker_buy': float(k['V']),
            'trades': {
                'n':        len(trades),
                'buy_vol':  round(buy_vol,  6),
                'sell_vol': round(sell_vol, 6),
                'cvd':      round(buy_vol - sell_vol, 6),
                'raw':      trades,   # compact [delta_ms, price_c, qty_u, side]
            },
            'book': {
                'ts':   self._book_ts,
                'bids': list(self._bids),   # [[price_cents, qty_units], ...]
                'asks': list(self._asks),
            },
        }

        # Day rollover
        day = self._utc_date()
        if day != self._today:
            log(f"Day rollover → {day}  ({len(self._bars)} bars saved)", G)
            path = _flush(self._today, self._bars)
            _git_push(path, len(self._bars), close_px)
            self._today = day
            self._bars  = []

        self._bars.append(bar)

        close_dt = datetime.fromtimestamp(int(k['T']) / 1000)
        n_trades = bar['trades']['n']
        log(f"Bar {close_dt.strftime('%H:%M')}  "
            f"c={close_px:.2f}  vol={bar['volume']:.3f}  "
            f"trades={n_trades}  ob={len(self._bids)}/{len(self._asks)}L  "
            f"total={len(self._bars)}", C)

        # Flush + push on every bar close
        path = _flush(self._today, self._bars)
        _git_push(path, len(self._bars), close_px)

    def stop(self):
        self._running = False

    async def run(self):
        log(f"Starting — {SYMBOL} {INTERVAL}  compress=gzip-{COMPRESS}", B)
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
                                    d  = json.loads(msg.data)
                                    st = d.get('stream', '')
                                    da = d.get('data',   {})
                                    if '@kline' in st:
                                        self._handle_kline(da.get('k', {}))
                                    elif '@depth' in st:
                                        self._handle_depth(da)
                                    elif 'aggTrade' in st:
                                        self._handle_trade(da)
                                except Exception as e:
                                    log(f"Parse: {e}", R)
                            elif msg.type in (aiohttp.WSMsgType.CLOSED,
                                              aiohttp.WSMsgType.ERROR):
                                log("WS closed — reconnecting", Y)
                                break
            except asyncio.CancelledError:
                break
            except Exception as e:
                if not self._running:
                    break
                log(f"Error ({e}) — retry in {backoff}s", R)
                await asyncio.sleep(backoff)
                backoff = min(backoff * 2, 60)

        if self._bars:
            path = _flush(self._today, self._bars)
            _git_push(path, len(self._bars), self._bars[-1]['close'])
        log("Stopped", Y)


# ── Replay helpers ────────────────────────────────────────────────────────────

def load_day(date_str: str) -> list:
    p = _day_path(date_str)
    if not p.exists():
        raise FileNotFoundError(p)
    with gzip.open(p, 'rt') as f:
        return json.load(f).get('bars', [])

def load_range(start: str, end: str) -> list:
    d, end_d = (datetime.strptime(s, '%Y-%m-%d') for s in (start, end))
    bars = []
    while d <= end_d:
        try: bars.extend(load_day(d.strftime('%Y-%m-%d')))
        except FileNotFoundError: pass
        d += timedelta(days=1)
    return sorted(bars, key=lambda b: b['ts'])

def summary():
    if not OUT_DIR.exists():
        print("No data collected."); return
    total_bars, total_sz = 0, 0
    print(f"\n{'─'*55}")
    for p in sorted(OUT_DIR.glob(f"{SYMBOL}_{INTERVAL}_*.json.gz")):
        m = re.search(r'(\d{4}-\d{2}-\d{2})', p.name)
        if not m: continue
        ds = m.group(1)
        sz = p.stat().st_size
        try: n = len(load_day(ds))
        except Exception: n = 0
        total_bars += n; total_sz += sz
        print(f"  {ds}  {n:4d} bars  {sz/1024:7.1f} kB  ({p.name})")
    print(f"{'─'*55}")
    print(f"  Total    {total_bars:4d} bars  {total_sz/1024:7.1f} kB\n")


# ── Entry point ───────────────────────────────────────────────────────────────

async def _main():
    c    = Collector()
    loop = asyncio.get_event_loop()
    for sig in (signal.SIGINT, signal.SIGTERM):
        loop.add_signal_handler(sig, c.stop)
    await c.run()

if __name__ == '__main__':
    if len(sys.argv) > 1 and sys.argv[1] == 'summary':
        summary()
    else:
        asyncio.run(_main())
