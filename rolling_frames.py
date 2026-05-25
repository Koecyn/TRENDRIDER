#!/usr/bin/env python3
"""
rolling_frames.py — live rolling candles across all timeframes from 1s data.

Bootstrap: fetch closed bars from Binance REST (1m/5m/15m/1h/4h).
           Each TF window pre-filled with (N-1) synthetic copies of last
           closed bar. Real 1s data fills in from the end, oldest drops off.

Window sizes (1s slots):
  1m  =    60
  5m  =   300
  15m =   900
  1h  =  3600
  4h  = 14400

Usage: python rolling_frames.py
"""

import gzip, json, os, subprocess, sys, time, urllib.request
from collections import deque
from datetime import datetime, timezone
from pathlib import Path

REPO     = Path(__file__).resolve().parent
RAW_FILE = REPO / "data" / "raw" / "BTCUSDC_LIVE.jsonl"
GZ_FILE  = REPO / "data" / "raw" / "BTCUSDC_LIVE.jsonl.gz"
REST     = "https://api.binance.us/api/v3/klines?symbol=BTCUSDT&interval={tf}&limit=2"
os.chdir(REPO)

TIMEFRAMES = {
    '1m':  60,
    '5m':  300,
    '15m': 900,
    '1h':  3600,
    '4h':  14400,
}

G='\033[92m'; Y='\033[93m'; C='\033[96m'; Z='\033[0m'
def log(m, c=Z): print(f"{c}{m}{Z}", flush=True)

# ── REST bootstrap ────────────────────────────────────────────────────────────

def fetch_last_bar(tf: str) -> dict | None:
    try:
        url = REST.format(tf=tf)
        with urllib.request.urlopen(url, timeout=10) as r:
            raw = json.loads(r.read())
        k = raw[-2]   # last CLOSED bar (raw[-1] is open)
        return {
            'ts':     int(k[0]),
            'open':   float(k[1]),
            'high':   float(k[2]),
            'low':    float(k[3]),
            'close':  float(k[4]),
            'volume': float(k[5]),
        }
    except Exception as e:
        log(f"  REST {tf} failed: {e}", Y)
        return None

def make_slot(bar: dict, vol_divisor: int) -> dict:
    """One synthetic 1s slot from a closed bar."""
    c = bar['close']
    return {
        'o': c, 'h': bar['high'], 'l': bar['low'], 'c': c,
        'vol': bar['volume'] / vol_divisor,
        'real': False,
    }

# ── Rolling window ────────────────────────────────────────────────────────────

class Window:
    def __init__(self, tf: str, n_slots: int):
        self.tf      = tf
        self.n       = n_slots
        self.slots   = deque(maxlen=n_slots)
        self.n_real  = 0

    def bootstrap(self, bar: dict):
        self.slots.clear()
        self.n_real = 0
        slot = make_slot(bar, self.n - 1)
        for _ in range(self.n - 1):
            self.slots.append(slot)
        log(f"  {self.tf:>3s}  bootstrapped {self.n-1} synthetic slots "
            f"from close={bar['close']:.2f}", Y)

    def add_second(self, s: dict):
        """s = {o, h, l, c, vol, real}"""
        dropped = self.slots[0] if self.slots else None
        self.slots.append(s)
        if not (dropped and not dropped['real']):
            # a real slot dropped off — keep count accurate
            pass
        self.n_real = sum(1 for x in self.slots if x['real'])

    def candle(self) -> dict | None:
        if not self.slots:
            return None
        sl = list(self.slots)
        return {
            'tf':     self.tf,
            'open':   sl[0]['o'],
            'high':   max(s['h'] for s in sl),
            'low':    min(s['l'] for s in sl),
            'close':  sl[-1]['c'],
            'volume': round(sum(s['vol'] for s in sl), 6),
            'real':   self.n_real,
            'synth':  self.n - self.n_real,
            'fill':   round(self.n_real / self.n * 100, 1),
        }


# ── 1s candle builder from raw trade lines ───────────────────────────────────

class SecBuilder:
    def __init__(self):
        self._sec    = 0
        self._trades = []
        self._sealed = []   # completed 1s candles

    def ingest(self, line: str):
        rec = json.loads(line)
        if rec[0] != 'T':
            return
        _, ts, p_c, q_u, _ = rec
        sec   = ts // 1000
        price = p_c / 100
        qty   = q_u / 10000

        if sec != self._sec and self._sec != 0:
            self._seal()
        if self._sec == 0 or sec != self._sec:
            self._sec = sec

        self._trades.append((price, qty))

    def _seal(self):
        if not self._trades:
            return
        prices = [t[0] for t in self._trades]
        vol    = sum(t[1] for t in self._trades)
        self._sealed.append({
            'ts':  self._sec * 1000,
            'o':   prices[0],
            'h':   max(prices),
            'l':   min(prices),
            'c':   prices[-1],
            'vol': round(vol, 6),
            'real': True,
        })
        self._trades = []

    def drain(self) -> list:
        out = list(self._sealed)
        self._sealed.clear()
        return out


# ── Main ──────────────────────────────────────────────────────────────────────

def read_raw_lines() -> list:
    """Read from local JSONL if exists, else fetch from git."""
    if RAW_FILE.exists() and RAW_FILE.stat().st_size > 0:
        return RAW_FILE.read_text().strip().split('\n')
    # fallback: read compressed from git
    subprocess.run(['git','fetch','origin','data/raw'], capture_output=True)
    r = subprocess.run(
        ['git','show','origin/data/raw:data/raw/BTCUSDC_LIVE.jsonl.gz'],
        capture_output=True)
    if not r.stdout:
        return []
    return gzip.decompress(r.stdout).decode().strip().split('\n')

def print_candles(windows: dict):
    now = datetime.utcnow().strftime('%H:%M:%S')
    print(f"\n{'─'*65}  {now} UTC")
    print(f"{'TF':>4}  {'open':>10}  {'high':>10}  {'low':>10}  "
          f"{'close':>10}  {'vol':>8}  {'fill':>6}")
    print('─'*65)
    for tf, w in windows.items():
        c = w.candle()
        if not c: continue
        bar = f"[{'█'*(c['real']*10//w.n):10s}]"
        print(f"{tf:>4}  {c['open']:>10.2f}  {c['high']:>10.2f}  "
              f"{c['low']:>10.2f}  {c['close']:>10.2f}  "
              f"{c['volume']:>8.4f}  {c['fill']:>5.1f}% {bar}")

def main():
    log("Bootstrap: fetching closed bars from Binance REST...", Y)
    windows = {}
    for tf, n_slots in TIMEFRAMES.items():
        bar = fetch_last_bar(tf)
        w   = Window(tf, n_slots)
        if bar:
            w.bootstrap(bar)
        windows[tf] = w

    log("\nReading raw stream...", G)
    builder   = SecBuilder()
    seen_lines = 0

    while True:
        lines = read_raw_lines()
        new   = lines[seen_lines:]
        if new:
            for line in new:
                if line:
                    builder.ingest(line)
            seen_lines = len(lines)

            for s in builder.drain():
                for w in windows.values():
                    w.add_second(s)

        print_candles(windows)
        time.sleep(1)

if __name__ == '__main__':
    main()
