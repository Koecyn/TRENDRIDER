#!/usr/bin/env python3
"""
build_candles.py — build 1s candles from raw stream data.

Reads data/raw/BTCUSDC_LIVE.jsonl.gz from the data/raw git branch.

Per 1-second bucket:
  candle: {ts, open, high, low, close, volume, n_trades, vwap}
  books:  [ [ts, bids, asks], ... ]  — every OB snapshot that second

Prints a summary and saves to data/raw/candles_1s.json.gz
"""

import gzip, json, os, subprocess, sys
from collections import defaultdict
from pathlib import Path

REPO    = Path(__file__).resolve().parent
GZ_OUT  = REPO / "data" / "raw" / "candles_1s.json.gz"
os.chdir(REPO)

def fetch():
    r = subprocess.run(
        ['git','show','origin/data/raw:data/raw/BTCUSDC_LIVE.jsonl.gz'],
        capture_output=True)
    if not r.stdout:
        print("No data on data/raw yet."); sys.exit(1)
    return gzip.decompress(r.stdout).decode().strip().split('\n')

def build(lines):
    # bucket by second
    trades  = defaultdict(list)   # sec → [price, ...]
    volumes = defaultdict(float)  # sec → total qty
    books   = defaultdict(list)   # sec → [[ts, bids, asks], ...]

    for line in lines:
        if not line: continue
        rec = json.loads(line)
        if rec[0] == 'T':
            _, ts, p_c, q_u, side = rec
            sec = ts // 1000
            price = p_c / 100
            qty   = q_u / 10000
            trades[sec].append(price)
            volumes[sec] += qty
        elif rec[0] == 'D':
            _, ts, bids, asks = rec
            sec = ts // 1000
            books[sec].append([ts, bids, asks])

    candles = []
    all_secs = sorted(set(list(trades.keys()) + list(books.keys())))
    for sec in all_secs:
        px = trades[sec]
        vol = volumes[sec]
        bk  = books[sec]
        candle = {
            'ts':       sec * 1000,
            'open':     px[0]  if px else None,
            'high':     max(px) if px else None,
            'low':      min(px) if px else None,
            'close':    px[-1] if px else None,
            'volume':   round(vol, 6),
            'n_trades': len(px),
            'vwap':     round(sum(px)/len(px), 2) if px else None,
        }
        candles.append({'candle': candle, 'books': bk})

    return candles

def save(candles):
    GZ_OUT.parent.mkdir(parents=True, exist_ok=True)
    tmp = GZ_OUT.with_suffix('.tmp.gz')
    with gzip.open(tmp, 'wt', compresslevel=9) as f:
        json.dump(candles, f, separators=(',',':'))
    tmp.rename(GZ_OUT)

def show(candles):
    trade_secs = [c for c in candles if c['candle']['n_trades'] > 0]
    print(f"\nTotal 1s buckets : {len(candles)}")
    print(f"Buckets w/ trades: {len(trade_secs)}")
    print(f"Buckets w/ OB    : {sum(1 for c in candles if c['books'])}")

    if trade_secs:
        import datetime
        print(f"\n--- CANDLES WITH TRADES ---")
        for c in trade_secs:
            ca = c['candle']
            ts = datetime.datetime.utcfromtimestamp(ca['ts']//1000).strftime('%H:%M:%S')
            print(f"  {ts}  O={ca['open']:.2f} H={ca['high']:.2f} "
                  f"L={ca['low']:.2f} C={ca['close']:.2f}  "
                  f"vol={ca['volume']:.5f}  n={ca['n_trades']}  vwap={ca['vwap']:.2f}")

    # Show latest OB snapshot
    for c in reversed(candles):
        if c['books']:
            import datetime
            ts, bids, asks = c['books'][-1]
            dt = datetime.datetime.utcfromtimestamp(ts//1000).strftime('%H:%M:%S')
            print(f"\n--- LATEST OB SNAPSHOT ({dt}) ---")
            print("  BIDS:")
            for p,q in bids[:5]:
                print(f"    ${p/100:,.2f}  {q/10000:.4f}")
            print("  ASKS:")
            for p,q in asks[:5]:
                print(f"    ${p/100:,.2f}  {q/10000:.4f}")
            break

if __name__ == '__main__':
    print("Fetching from data/raw...", flush=True)
    subprocess.run(['git','fetch','origin','data/raw'], capture_output=True)
    lines   = fetch()
    candles = build(lines)
    save(candles)
    show(candles)
    print(f"\nSaved → {GZ_OUT.name}")
