#!/usr/bin/env python3
"""
collect_raw.py — raw market data collector for BTCUSDC.

STARTUP: pulls last 300 closed 1m bars from Binance REST (bootstrap).
LIVE:    streams aggTrades + depth20 snapshots via WebSocket.
PUSH:    every PUSH_INTERVAL seconds — gzip-9 compressed file to data/live.

Trade record: [delta_ms, price_cents, qty_units, side]
OB snapshot:  {ts, bids:[[price_cents,qty_units],...], asks:[...]}

Usage:
  python collect_raw.py          # run collector
  python collect_raw.py summary  # show collected files
"""

import asyncio
import gzip
import json
import signal
import subprocess
import sys
import time
import urllib.request
from datetime import datetime, timezone, timedelta
from pathlib import Path

try:
    import aiohttp
except ImportError:
    print("ERROR: pip install aiohttp"); sys.exit(1)

# ── Config ────────────────────────────────────────────────────────────────────

SYMBOL        = "BTCUSDC"
INTERVAL      = "1m"
REPO          = Path(__file__).resolve().parent
OUT_DIR       = REPO / "data" / "raw"
DATA_BRANCH   = "data/live"
COMPRESS      = 9
PUSH_INTERVAL = 5      # push to git every N seconds
BOOTSTRAP_N   = 300    # bars to pull from REST at startup

WS_URL = ("wss://stream.binance.us:9443/stream"
          "?streams=btcusdc@kline_1m/btcusdc@depth20@100ms/btcusdc@aggTrade")
REST_URL = ("https://api.binance.us/api/v3/klines"
            "?symbol=BTCUSDC&interval=1m&limit={n}")

# ── Helpers ───────────────────────────────────────────────────────────────────

G='\033[92m'; R='\033[91m'; Y='\033[93m'; C='\033[96m'; B='\033[1m'; Z='\033[0m'

def ts_str(): return datetime.now().strftime('%H:%M:%S')
def log(msg, col=Z): print(f"{col}[raw {ts_str()}] {msg}{Z}", flush=True)

def git(*args):
    return subprocess.run(['git','-C',str(REPO)]+list(args),
                          capture_output=True, text=True)

def _day_path(date_str):
    return OUT_DIR / f"{SYMBOL}_{INTERVAL}_{date_str}.json.gz"

def _write(date_str, closed_bars, live_trades, live_ob):
    OUT_DIR.mkdir(parents=True, exist_ok=True)
    p   = _day_path(date_str)
    tmp = p.with_suffix('.tmp.gz')
    with gzip.open(tmp, 'wt', compresslevel=COMPRESS) as f:
        json.dump({
            'symbol':      SYMBOL,
            'interval':    INTERVAL,
            'date':        date_str,
            'bars':        closed_bars,
            'live_trades': live_trades,   # trades in current open bar
            'live_ob':     live_ob,       # latest OB snapshot
        }, f, separators=(',',':'))
    tmp.rename(p)
    return p

def _push(path, n_bars, n_trades):
    rel = str(path.relative_to(REPO))
    git('add', rel)
    r = git('commit', '-m',
            f"raw {ts_str()} bars={n_bars} trades={n_trades}")
    if r.returncode != 0:
        return  # nothing changed — no new data since last push
    pr = git('push', 'origin', f'HEAD:{DATA_BRANCH}')
    sz = path.stat().st_size
    if pr.returncode == 0:
        log(f"pushed {path.name}  {sz/1024:.1f}kB  "
            f"bars={n_bars} live_trades={n_trades}", G)
    else:
        log(f"push failed: {pr.stderr.strip()}", R)

def _bootstrap():
    """Pull last BOOTSTRAP_N closed 1m bars from Binance REST."""
    log(f"Bootstrap: fetching {BOOTSTRAP_N} bars from REST...", Y)
    try:
        url = REST_URL.format(n=BOOTSTRAP_N + 1)
        with urllib.request.urlopen(url, timeout=15) as r:
            raw = json.loads(r.read())
        bars = []
        for k in raw[:-1]:  # drop last (open bar)
            bars.append({
                'ts':        int(k[0]),
                'open':      float(k[1]),
                'high':      float(k[2]),
                'low':       float(k[3]),
                'close':     float(k[4]),
                'volume':    float(k[5]),
                'taker_buy': float(k[9]),
                'trades':    None,   # no tick data for historical bars
                'book':      None,
            })
        log(f"Bootstrap: {len(bars)} bars  "
            f"last={bars[-1]['close']:.2f}  "
            f"ts={datetime.utcfromtimestamp(bars[-1]['ts']//1000).strftime('%H:%M')}", G)
        return bars
    except Exception as e:
        log(f"Bootstrap failed ({e}) — starting live only", R)
        return []


# ── Collector ─────────────────────────────────────────────────────────────────

class Collector:

    def __init__(self, bootstrap_bars):
        self._running       = True
        self._today         = self._utc_date()

        # Closed bars for today — seed with bootstrap if same day
        self._closed: list  = []
        if bootstrap_bars:
            today_ms = self._today_start_ms()
            for b in bootstrap_bars:
                if b['ts'] >= today_ms:
                    self._closed.append(b)
            log(f"Today's bars from bootstrap: {len(self._closed)}", C)

        # Current open bar accumulators
        self._bar_open_ms:  int   = 0
        self._bar_trades:   list  = []   # [delta_ms, price_c, qty_u, side]

        # Latest OB snapshot
        self._ob:           dict  = {}
        self._book_ts:      int   = 0

        self._last_push:    float = 0.0

    @staticmethod
    def _utc_date():
        return datetime.now(timezone.utc).strftime('%Y-%m-%d')

    @staticmethod
    def _today_start_ms():
        now = datetime.now(timezone.utc)
        d   = now.replace(hour=0, minute=0, second=0, microsecond=0)
        return int(d.timestamp() * 1000)

    # ── WebSocket handlers ────────────────────────────────────────────────────

    def on_depth(self, data):
        self._ob = {
            'ts':   int(time.time() * 1000),
            'bids': [[int(float(p)*100), int(float(q)*10000)]
                     for p, q in data.get('bids', [])],
            'asks': [[int(float(p)*100), int(float(q)*10000)]
                     for p, q in data.get('asks', [])],
        }

    def on_trade(self, data):
        ts_ms = int(data['T'])
        if self._bar_open_ms == 0:
            self._bar_open_ms = ts_ms
        delta = max(0, ts_ms - self._bar_open_ms)
        self._bar_trades.append([
            delta,
            int(float(data['p']) * 100),
            int(float(data['q']) * 10000),
            0 if data.get('m') else 1,
        ])

    def on_kline(self, k):
        ts_open = int(k['t'])
        if self._bar_open_ms == 0:
            self._bar_open_ms = ts_open

        if k.get('x', False):
            # Bar closed — seal it
            trades   = list(self._bar_trades)
            self._bar_trades  = []
            self._bar_open_ms = 0

            buy_vol  = sum(t[2] for t in trades if t[3]==1) / 10000.0
            sell_vol = sum(t[2] for t in trades if t[3]==0) / 10000.0

            bar = {
                'ts':        int(k['t']),
                'open':      float(k['o']),
                'high':      float(k['h']),
                'low':       float(k['l']),
                'close':     float(k['c']),
                'volume':    float(k['v']),
                'taker_buy': float(k['V']),
                'trades': {
                    'n':        len(trades),
                    'buy_vol':  round(buy_vol, 6),
                    'sell_vol': round(sell_vol, 6),
                    'cvd':      round(buy_vol - sell_vol, 6),
                    'raw':      trades,
                },
                'book': dict(self._ob),
            }

            # Day rollover
            day = self._utc_date()
            if day != self._today:
                log(f"Day rollover {self._today}→{day}", Y)
                self._closed = []
                self._today  = day

            self._closed.append(bar)
            log(f"bar closed  {datetime.utcfromtimestamp(int(k['T'])//1000).strftime('%H:%M')}  "
                f"c={bar['close']:.2f}  trades={len(trades)}  "
                f"total_bars={len(self._closed)}", C)

            # Always push immediately on bar close
            self._do_push()

    def _do_push(self):
        path = _write(self._today, self._closed,
                      list(self._bar_trades), dict(self._ob))
        n_trades = len(self._bar_trades)
        _push(path, len(self._closed), n_trades)
        self._last_push = time.time()

    def tick(self):
        """Called every second — push if PUSH_INTERVAL elapsed."""
        if time.time() - self._last_push >= PUSH_INTERVAL:
            self._do_push()

    def stop(self):
        self._running = False

    # ── Main loop ─────────────────────────────────────────────────────────────

    async def run(self):
        backoff = 1
        while self._running:
            try:
                async with aiohttp.ClientSession() as sess:
                    async with sess.ws_connect(
                            WS_URL, heartbeat=20, receive_timeout=30) as ws:
                        backoff = 1
                        log("Connected", G)
                        last_tick = time.time()
                        async for msg in ws:
                            if not self._running:
                                break
                            now = time.time()
                            if now - last_tick >= 1.0:
                                self.tick()
                                last_tick = now
                            if msg.type == aiohttp.WSMsgType.TEXT:
                                try:
                                    d  = json.loads(msg.data)
                                    st = d.get('stream','')
                                    da = d.get('data',{})
                                    if 'aggTrade' in st:
                                        self.on_trade(da)
                                    elif '@depth' in st:
                                        self.on_depth(da)
                                    elif '@kline' in st:
                                        self.on_kline(da.get('k',{}))
                                except Exception as e:
                                    log(f"parse: {e}", R)
                            elif msg.type in (aiohttp.WSMsgType.CLOSED,
                                              aiohttp.WSMsgType.ERROR):
                                break
            except asyncio.CancelledError:
                break
            except Exception as e:
                if not self._running: break
                log(f"error ({e}) — retry in {backoff}s", R)
                await asyncio.sleep(backoff)
                backoff = min(backoff * 2, 60)

        self._do_push()
        log("Stopped", Y)


# ── Entry point ───────────────────────────────────────────────────────────────

async def _main():
    bootstrap = _bootstrap()
    c    = Collector(bootstrap)
    loop = asyncio.get_event_loop()
    for sig in (signal.SIGINT, signal.SIGTERM):
        loop.add_signal_handler(sig, c.stop)
    log(f"Pushing every {PUSH_INTERVAL}s + on every bar close", B)
    await c.run()

def summary():
    from pathlib import Path
    import re
    if not OUT_DIR.exists():
        print("No data."); return
    for p in sorted(OUT_DIR.glob(f"{SYMBOL}_{INTERVAL}_*.json.gz")):
        m = re.search(r'(\d{4}-\d{2}-\d{2})', p.name)
        if not m: continue
        with gzip.open(p,'rt') as f:
            d = json.load(f)
        bars = d.get('bars',[])
        print(f"{m.group(1)}  {len(bars):4d} bars  {p.stat().st_size/1024:.1f}kB")

if __name__ == '__main__':
    if len(sys.argv) > 1 and sys.argv[1] == 'summary':
        summary()
    else:
        asyncio.run(_main())
