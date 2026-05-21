"""
Binance public REST data fetching — no auth required.

Primary endpoint: GET /api/v3/klines
  k[0]  openTime (ms)
  k[1]  open
  k[2]  high
  k[3]  low
  k[4]  close
  k[5]  volume (base asset)
  k[9]  takerBuyBaseAssetVolume  ← real CVD input

Backtest data is saved to CSV so Termux doesn't re-fetch every run.
"""

import csv
import json
import time
import urllib.error
import urllib.request
from pathlib import Path

import numpy as np

from . import config as C

_BASE = "https://api.binance.us/api/v3"


# ─────────────────────────────────────────────────────────────────────────────
# Fetch
# ─────────────────────────────────────────────────────────────────────────────

def _get(url: str, retries: int = 3) -> list | dict:
    for attempt in range(retries):
        try:
            with urllib.request.urlopen(url, timeout=10) as r:
                return json.loads(r.read())
        except urllib.error.URLError as e:
            if attempt < retries - 1:
                time.sleep(2 ** attempt)
            else:
                raise RuntimeError(f"Binance fetch failed: {e}  url={url}")


def fetch_klines(symbol: str = None, interval: str = None,
                 limit: int = 1000, end_time: int = None) -> list:
    sym = symbol or C.SYMBOL
    ivl = interval or C.INTERVAL
    url = f"{_BASE}/klines?symbol={sym}&interval={ivl}&limit={limit}"
    if end_time:
        url += f"&endTime={end_time}"
    return _get(url)


def fetch_klines_bulk(symbol: str = None, interval: str = None,
                      total_bars: int = None) -> list:
    """Fetch multiple pages going backwards in time."""
    n    = total_bars or C.HISTORY_BARS
    sym  = symbol or C.SYMBOL
    ivl  = interval or C.INTERVAL
    bars = []
    end  = None

    while len(bars) < n:
        need  = min(1000, n - len(bars))
        batch = fetch_klines(sym, ivl, need, end)
        if not batch:
            break
        bars = batch + bars
        end  = int(batch[0][0]) - 1
        if len(batch) < need:
            break
        time.sleep(0.25)

    return bars[-n:]


def fetch_orderbook(symbol: str = None, limit: int = 20) -> dict:
    sym = symbol or C.SYMBOL
    raw = _get(f"{_BASE}/depth?symbol={sym}&limit={limit}")
    return {
        'bids': [(float(p), float(q)) for p, q in raw['bids']],
        'asks': [(float(p), float(q)) for p, q in raw['asks']],
    }


# ─────────────────────────────────────────────────────────────────────────────
# Parse
# ─────────────────────────────────────────────────────────────────────────────

_COLS = ['timestamps', 'opens', 'highs', 'lows', 'closes',
         'volumes', 'taker_buy']

def parse_klines(raw: list) -> dict:
    """Convert raw Binance kline list → dict of numpy arrays."""
    if not raw:
        return {}
    ts  = np.array([int(k[0])   for k in raw], dtype=np.int64)
    o   = np.array([float(k[1]) for k in raw])
    h   = np.array([float(k[2]) for k in raw])
    l   = np.array([float(k[3]) for k in raw])
    c   = np.array([float(k[4]) for k in raw])
    v   = np.array([float(k[5]) for k in raw])
    tb  = np.array([float(k[9]) for k in raw])
    return {
        'timestamps': ts,
        'opens':      o,
        'highs':      h,
        'lows':       l,
        'closes':     c,
        'prices':     c,
        'volumes':    v,
        'taker_buy':  tb,
    }


# ─────────────────────────────────────────────────────────────────────────────
# CSV cache
# ─────────────────────────────────────────────────────────────────────────────

def save_csv(data: dict, path: str):
    with open(path, 'w', newline='') as f:
        w = csv.writer(f)
        w.writerow(_COLS)
        n = len(data['timestamps'])
        for i in range(n):
            w.writerow([data[c][i] for c in _COLS])


def load_csv(path: str) -> dict | None:
    p = Path(path)
    if not p.exists():
        return None
    rows = []
    with open(path, 'r') as f:
        for row in csv.DictReader(f):
            rows.append({k: float(v) for k, v in row.items()})
    if not rows:
        return None
    d = {k: np.array([r[k] for r in rows]) for k in _COLS}
    d['timestamps'] = d['timestamps'].astype(np.int64)
    d['prices']     = d['closes']
    return d


def get_data(cache_path: str = None, symbol: str = None,
             interval: str = None, total_bars: int = None,
             refresh: bool = False) -> dict:
    """
    Load from CSV cache if available, otherwise fetch and save.
    Pass refresh=True to force re-fetch.
    """
    cp = cache_path or f"physics_data_{(symbol or C.SYMBOL).lower()}.csv"
    if not refresh:
        cached = load_csv(cp)
        if cached:
            return cached
    raw  = fetch_klines_bulk(symbol, interval, total_bars)
    data = parse_klines(raw)
    save_csv(data, cp)
    return data


def get_data_phyx(phyx_dir: str, symbol: str = None,
                  ts_from: int = 0, ts_to: int = None,
                  fallback_rest: bool = True) -> dict:
    """
    Load kline data from PHYX compressed files (collected by physics.collector).
    Falls back to Binance REST if no PHYX files found and fallback_rest=True.

    PHYX files are preferred when available — they contain real order book
    snapshots captured live, which the REST endpoint cannot provide historically.
    """
    sym = symbol or C.SYMBOL
    try:
        from .store import load_klines
        data = load_klines(phyx_dir, sym, ts_from, ts_to)
        if data and len(data.get('closes', [])) >= C.WARMUP_BARS:
            return data
    except Exception as e:
        pass  # store module may not be importable if deps missing

    if fallback_rest:
        return get_data(symbol=sym)
    return {}


def load_ob_snapshots(phyx_dir: str, symbol: str = None,
                      ts_from: int = 0, ts_to: int = None) -> list:
    """
    Load order book snapshots from PHYX files.
    Returns list of {'ts_ms', 'mid', 'bids', 'asks'} dicts in time order.
    Bids/asks are [(price, qty), ...] — level 20 each side.
    """
    sym = symbol or C.SYMBOL
    try:
        from .store import iter_files
        return [r for r in iter_files(phyx_dir, sym, ts_from, ts_to)
                if r['type'] == 'orderbook']
    except Exception:
        return []
