"""
physics/collector.py — Live WebSocket data collector.

Connects to Binance combined stream (no auth required):
  <symbol>@kline_1m          — 1-min OHLCV + taker buy/sell
  <symbol>@depth20@100ms     — top 20 bids + top 20 asks, every 100ms

Both streams run concurrently in a single asyncio event loop.
Data is written to PHYX compressed files, rolled every hour.

Usage (Termux):
  python -m physics.collector                     # BTCUSDC, ./phyx_data/
  python -m physics.collector --symbol BTCUSDT --data-dir /sdcard/phyx

Install deps:
  pip install zstandard msgpack aiohttp

Storage estimate @ 500ms OB snapshots + 1m klines, zstd level 9:
  ~15-25 MB/day  (~450-750 MB/month)
"""

import asyncio
import json
import logging
import os
import signal
import time
from datetime import datetime, timezone
from pathlib import Path

try:
    import aiohttp
    _AIOHTTP = True
except ImportError:
    _AIOHTTP = False

from .store import PhyxWriter, _best_algo, ALGO_ZSTD

log = logging.getLogger('phyx.collector')

# ── Binance stream URLs ───────────────────────────────────────────────────────
_WS_BASE = {
    'binance':    'wss://stream.binance.com:9443/stream',
    'binanceus':  'wss://stream.binance.us:9443/stream',
}

# ── Defaults ──────────────────────────────────────────────────────────────────
DEFAULT_SYMBOL   = 'BTCUSDC'
DEFAULT_DATA_DIR = 'phyx_data'
DEFAULT_EXCHANGE = 'binance'
OB_STORE_EVERY_MS = 500    # store OB snapshot every N ms (100 = every tick)
FLUSH_EVERY       = 500    # records per compressed chunk
ZSTD_LEVEL        = 9      # compression aggressiveness (1=fast, 22=max)
FILE_ROLL_SECS    = 3600   # roll to new file every hour


class Collector:
    """
    Subscribes to kline_1m + depth20@100ms for a symbol.
    Writes PHYX compressed files, rolls hourly.
    """

    def __init__(self, symbol: str = DEFAULT_SYMBOL,
                 data_dir: str = DEFAULT_DATA_DIR,
                 exchange: str = DEFAULT_EXCHANGE,
                 ob_every_ms: int = OB_STORE_EVERY_MS,
                 zstd_level: int = ZSTD_LEVEL):
        self._sym       = symbol.upper()
        self._dir       = Path(data_dir)
        self._dir.mkdir(parents=True, exist_ok=True)
        self._ex        = exchange
        self._ob_every  = ob_every_ms
        self._level     = zstd_level

        self._writer: PhyxWriter | None = None
        self._file_opened: float = 0.0   # unix time file was opened
        self._last_ob_ts: int = 0        # last OB record timestamp stored

        self._running   = True
        self._stats     = {'klines': 0, 'ob_snaps': 0, 'reconnects': 0}

    # ── File management ───────────────────────────────────────────────────────

    def _open_file(self):
        if self._writer is not None:
            self._writer.close()
        now  = datetime.now(timezone.utc)
        name = f"{self._sym}_{now.strftime('%Y%m%d_%H')}.phyx"
        path = self._dir / name
        self._writer      = PhyxWriter(
            str(path), symbol=self._sym,
            algo=_best_algo(), flush_every=FLUSH_EVERY,
        )
        self._file_opened = time.time()
        log.info('Opened %s', path)

    def _maybe_roll(self):
        if time.time() - self._file_opened >= FILE_ROLL_SECS:
            log.info('Rolling file (1h)')
            self._open_file()

    # ── WebSocket URL ─────────────────────────────────────────────────────────

    def _ws_url(self) -> str:
        sym     = self._sym.lower()
        base    = _WS_BASE.get(self._ex, _WS_BASE['binance'])
        streams = f'{sym}@kline_1m/{sym}@depth20@100ms'
        return f'{base}?streams={streams}'

    # ── Message handlers ──────────────────────────────────────────────────────

    def _handle(self, msg: dict):
        stream = msg.get('stream', '')
        data   = msg.get('data', {})

        if '@kline' in stream:
            self._handle_kline(data)
        elif '@depth' in stream:
            self._handle_depth(data)

    def _handle_kline(self, data: dict):
        k  = data.get('k', {})
        if not k:
            return
        ts = int(k['t'])          # open time ms
        self._writer.write_kline(
            ts,
            float(k['o']),        # open
            float(k['h']),        # high
            float(k['l']),        # low
            float(k['c']),        # close
            float(k['v']),        # total volume
            float(k['V']),        # taker buy base volume  ← CVD input
        )
        self._stats['klines'] += 1

    def _handle_depth(self, data: dict):
        now = int(time.time() * 1000)
        if now - self._last_ob_ts < self._ob_every:
            return                # throttle to ob_every_ms

        bids = [(float(p), float(q)) for p, q in data.get('bids', [])]
        asks = [(float(p), float(q)) for p, q in data.get('asks', [])]
        if not bids or not asks:
            return

        self._writer.write_orderbook(now, bids, asks)
        self._last_ob_ts = now
        self._stats['ob_snaps'] += 1

    # ── Main loop ─────────────────────────────────────────────────────────────

    async def run(self):
        if not _AIOHTTP:
            raise RuntimeError('pip install aiohttp')

        self._open_file()
        url     = self._ws_url()
        backoff = 1

        log.info('Connecting → %s', url)

        while self._running:
            try:
                async with aiohttp.ClientSession() as sess:
                    async with sess.ws_connect(
                        url,
                        heartbeat=20,
                        receive_timeout=60,
                    ) as ws:
                        backoff = 1
                        log.info('Connected')
                        async for msg in ws:
                            if msg.type == aiohttp.WSMsgType.TEXT:
                                try:
                                    self._handle(json.loads(msg.data))
                                    self._maybe_roll()
                                except Exception as e:
                                    log.warning('Parse error: %s', e)
                            elif msg.type in (aiohttp.WSMsgType.CLOSED,
                                              aiohttp.WSMsgType.ERROR):
                                log.warning('WS closed/error: %s', msg)
                                break
            except asyncio.CancelledError:
                break
            except Exception as e:
                if not self._running:
                    break
                self._stats['reconnects'] += 1
                log.error('Connection error (%s) — reconnecting in %ds', e, backoff)
                await asyncio.sleep(backoff)
                backoff = min(backoff * 2, 60)

        if self._writer:
            self._writer.close()
        log.info('Collector stopped. Stats: %s', self._stats)

    def stop(self):
        self._running = False
        log.info('Stop requested')


# ── Storage report ────────────────────────────────────────────────────────────

def storage_report(data_dir: str = DEFAULT_DATA_DIR):
    """Print size summary of all PHYX files."""
    d     = Path(data_dir)
    files = sorted(d.glob('*.phyx'))
    if not files:
        print('No PHYX files found in', data_dir)
        return

    total = 0
    print(f'{"File":<40} {"Size":>10}  {"Chunks":>8}')
    print('-' * 62)
    for f in files:
        sz = f.stat().st_size
        total += sz
        # Read header for chunk count
        try:
            from .store import PhyxReader
            with PhyxReader(str(f)) as r:
                nc = r.info['n_chunks']
                sym = r.info['symbol']
        except Exception:
            nc = '?'; sym = '?'
        print(f'{f.name:<40} {sz/1024:>9.1f}K  {nc:>8}')
    print('-' * 62)
    print(f'{"TOTAL":<40} {total/1024/1024:>9.2f}M')


# ── CLI entry ────────────────────────────────────────────────────────────────

async def _main(symbol: str, data_dir: str, exchange: str,
                ob_every: int, zstd_level: int):
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s [%(levelname)s] %(message)s',
        datefmt='%H:%M:%S',
    )
    c = Collector(symbol, data_dir, exchange, ob_every, zstd_level)

    loop = asyncio.get_event_loop()
    for sig in (signal.SIGINT, signal.SIGTERM):
        loop.add_signal_handler(sig, c.stop)

    await c.run()


if __name__ == '__main__':
    import argparse
    ap = argparse.ArgumentParser(description='PHYX live data collector')
    ap.add_argument('--symbol',     default=DEFAULT_SYMBOL)
    ap.add_argument('--data-dir',   default=DEFAULT_DATA_DIR)
    ap.add_argument('--exchange',   default=DEFAULT_EXCHANGE,
                    choices=['binance', 'binanceus'])
    ap.add_argument('--ob-every',   type=int, default=OB_STORE_EVERY_MS,
                    help='ms between OB snapshots (default 500)')
    ap.add_argument('--zstd-level', type=int, default=ZSTD_LEVEL,
                    help='zstd compression level 1-22 (default 9)')
    ap.add_argument('--report',     action='store_true',
                    help='Print storage report and exit')
    args = ap.parse_args()

    if args.report:
        storage_report(args.data_dir)
    else:
        asyncio.run(_main(args.symbol, args.data_dir, args.exchange,
                          args.ob_every, args.zstd_level))
