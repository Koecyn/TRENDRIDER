#!/usr/bin/env python3
"""
ingest.py — Pull historical candles from Binance.US to fill data gaps.

Fetches 1m, 1h, and 4h candles covering the gap between the last stored
bar and now. Saves to ~/.trendrider/ for use by build_candles.py.

Standalone:  python3 ingest.py
             python3 ingest.py --mins 60    # fetch last N minutes of 1m only
Import:      import ingest; ingest.run()
"""

import gzip, json, sys, time, urllib.request
from pathlib import Path

DATA_DIR = Path.home() / '.trendrider'
BASE_URL = 'https://api.binance.us'
SYMBOL   = 'BTCUSDT'
MAX_BARS = 1000   # Binance limit per request

_INTERVAL_MS = {
    '1m':  60_000,
    '5m':  300_000,
    '15m': 900_000,
    '1h':  3_600_000,
    '4h':  14_400_000,
}

_FNAMES = {
    '1m':  'BTCUSDT_1m.json.gz',
    '1h':  'BTCUSDT_1h.json.gz',
    '4h':  'BTCUSDT_4h.json.gz',
}


def _fetch(interval: str, start_ms: int, end_ms: int) -> list:
    """Fetch klines [open_ms, o, h, l, c, vol, taker_buy_vol] from exchange."""
    step  = _INTERVAL_MS[interval]
    bars  = []
    cur   = start_ms
    while cur < end_ms:
        limit = min(MAX_BARS, (end_ms - cur) // step + 1)
        if limit <= 0:
            break
        url = (f'{BASE_URL}/api/v3/klines?symbol={SYMBOL}'
               f'&interval={interval}&startTime={cur}&endTime={end_ms}&limit={limit}')
        try:
            with urllib.request.urlopen(url, timeout=15) as r:
                chunk = json.loads(r.read())
        except Exception as e:
            print(f'[ingest] fetch error ({interval}): {e}', flush=True)
            break
        if not chunk:
            break
        for k in chunk:
            bars.append([int(k[0]), float(k[1]), float(k[2]),
                         float(k[3]), float(k[4]), float(k[5]), float(k[9])])
        last_open = chunk[-1][0]
        if last_open <= cur:
            break
        cur = last_open + step
    return bars


def _load(fname: str) -> list:
    p = DATA_DIR / fname
    if p.exists():
        try:
            with gzip.open(p, 'rt') as f:
                return json.loads(f.read())
        except Exception:
            pass
    return []


def _save(fname: str, bars: list):
    DATA_DIR.mkdir(parents=True, exist_ok=True)
    with gzip.open(DATA_DIR / fname, 'wt') as f:
        json.dump(bars, f)


def _merge(existing: list, new: list) -> list:
    merged = {k[0]: k for k in existing}
    for k in new:
        merged[k[0]] = k
    return sorted(merged.values(), key=lambda k: k[0])


def run(verbose: bool = True, mins: int = None) -> dict:
    """
    Fetch all intervals and save locally.
    Returns dict of {interval: n_new_bars}.
    """
    def log(m):
        if verbose:
            print(f'[ingest] {m}', flush=True)

    DATA_DIR.mkdir(parents=True, exist_ok=True)
    now_ms = int(time.time() * 1000)
    end_ms = now_ms - 60_000   # exclude the live (unclosed) bar

    results = {}

    for interval, fname in _FNAMES.items():
        existing = _load(fname)

        if mins and interval == '1m':
            start_ms = end_ms - mins * 60_000
        elif existing:
            last_ts   = existing[-1][0]
            step      = _INTERVAL_MS[interval]
            start_ms  = last_ts + step
        else:
            # Default lookback: enough to seed equations
            lookbacks = {'1m': 1440, '1h': 200, '4h': 90}
            start_ms  = end_ms - lookbacks[interval] * _INTERVAL_MS[interval]

        gap_bars = max(0, (end_ms - start_ms) // _INTERVAL_MS[interval])
        if gap_bars == 0:
            log(f'{interval}: up to date')
            results[interval] = 0
            continue

        log(f'{interval}: fetching {gap_bars} bars ...')
        new_bars = _fetch(interval, start_ms, end_ms)
        combined = _merge(existing, new_bars)
        _save(fname, combined)
        log(f'{interval}: {len(new_bars)} new  (total {len(combined)})')
        results[interval] = len(new_bars)

    return results


if __name__ == '__main__':
    import argparse
    p = argparse.ArgumentParser()
    p.add_argument('--mins', type=int, default=None,
                   help='Fetch only last N minutes of 1m data')
    a = p.parse_args()
    r = run(mins=a.mins)
    print(f'Done: {r}')
