#!/usr/bin/env python3
"""
build_candles.py — build 1m candles from raw BTCUSDT stream data.

Reads data/raw/BTCUSDT_LIVE.jsonl.gz from the data/raw git branch.

Session detection: splits raw data on gaps > 60s, uses the largest
continuous block. Forward-fills price AND ob_depth within the block only.

Per 1m candle:
  o/h/l/c   — OHLCV from aggregated 1s buckets
  vol       — BTC trade volume (flow signals: water_hammer, iceberg, dark_pool)
  ob        — mean top-5 bid+ask BTC depth per bar
  eff_usd   — USD notional, OB-filled where no trades
              pass this to reynolds(), shock_front(), cavitation()

Saves to data/raw/candles_1s.json.gz
"""

import gzip, json, os, subprocess, sys
import numpy as np
from collections import defaultdict
from pathlib import Path

REPO   = Path(__file__).resolve().parent
GZ_OUT = REPO / "data" / "raw" / "candles_1s.json.gz"
os.chdir(REPO)

GAP_THRESH = 60   # seconds — gap larger than this splits sessions


def fetch():
    r = subprocess.run(
        ['git','show','origin/data/raw:data/raw/BTCUSDT_LIVE.jsonl.gz'],
        capture_output=True)
    if not r.stdout:
        print("No BTCUSDT data on data/raw yet."); sys.exit(1)
    return gzip.decompress(r.stdout).decode().strip().split('\n')


def largest_block(trade_by_sec, ob_by_sec):
    """Return (t_start, t_end) of the largest continuous data block."""
    real_secs = sorted(set(list(trade_by_sec.keys()) + list(ob_by_sec.keys())))
    if not real_secs:
        return None, None
    blocks = []; cur = [real_secs[0]]
    for a, b in zip(real_secs, real_secs[1:]):
        if b - a > GAP_THRESH:
            blocks.append(cur); cur = []
        cur.append(b)
    blocks.append(cur)
    best = max(blocks, key=lambda bl: bl[-1] - bl[0])
    import datetime
    for bl in blocks:
        t0 = datetime.datetime.utcfromtimestamp(bl[0]).strftime('%H:%M:%S')
        t1 = datetime.datetime.utcfromtimestamp(bl[-1]).strftime('%H:%M:%S')
        flag = " <- selected" if bl is best else ""
        print(f"  block {t0}-{t1} UTC  {(bl[-1]-bl[0])//60}m{flag}")
    return best[0], best[-1]


def build(lines):
    trade_by_sec = defaultdict(list)
    ob_by_sec    = defaultdict(list)

    for line in lines:
        if not line: continue
        rec = json.loads(line)
        if rec[0] == 'T':
            _, ts, p_c, q_u, side = rec
            trade_by_sec[ts//1000].append((p_c/100, q_u/10000))
        elif rec[0] == 'D':
            _, ts, bids, asks = rec
            sec = ts//1000
            depth = (sum(q/10000 for _,q in bids[:5]) +
                     sum(q/10000 for _,q in asks[:5]))
            ob_by_sec[sec].append(depth)

    t0, t1 = largest_block(trade_by_sec, ob_by_sec)
    if t0 is None:
        return []

    # Build 1s candles with forward-fill within block only
    last_close = None; last_ob = None
    candles_1s = []
    for sec in range(t0, t1 + 1):
        if sec in trade_by_sec:
            px = [t[0] for t in trade_by_sec[sec]]
            vl = [t[1] for t in trade_by_sec[sec]]
            o,h,l,c = px[0], max(px), min(px), px[-1]
            vol = sum(vl); last_close = c
        else:
            if last_close is None: continue
            o=h=l=c=last_close; vol=0.0
        if sec in ob_by_sec:
            last_ob = float(np.mean(ob_by_sec[sec]))
        ob = last_ob if last_ob is not None else 0.0
        candles_1s.append({'ts':sec*1000,'o':o,'h':h,'l':l,'c':c,
                           'vol':round(vol,6),'ob':round(ob,4)})

    # Aggregate to 1m
    min_by = defaultdict(list)
    for c in candles_1s:
        min_by[c['ts']//60000].append(c)

    candles_1m = []
    for m in sorted(min_by):
        sl = min_by[m]
        trade_vol = sum(s['vol'] for s in sl)
        mid_price = sl[len(sl)//2]['c']
        mean_ob   = float(np.mean([s['ob'] for s in sl]))
        candles_1m.append({
            'ts':        m * 60000,
            'o':         sl[0]['o'],
            'h':         max(s['h'] for s in sl),
            'l':         min(s['l'] for s in sl),
            'c':         sl[-1]['c'],
            'vol':       round(trade_vol, 6),
            'ob':        round(mean_ob, 4),
            'trade_usd': round(trade_vol * mid_price, 2),
            'ob_usd':    round(mean_ob   * mid_price, 2),
        })

    # Scale factor: match OB-derived USD to trade USD distribution
    active_t = [c['trade_usd'] for c in candles_1m if c['trade_usd'] > 0]
    active_o = [c['ob_usd']    for c in candles_1m if c['ob_usd'] > 0]
    scale = (float(np.median(active_t)) / float(np.median(active_o))
             if active_t and active_o else 1.0)

    for c in candles_1m:
        c['eff_usd'] = round(
            c['trade_usd'] if c['trade_usd'] > 0 else c['ob_usd'] * scale, 2)

    return candles_1m


def save(candles):
    GZ_OUT.parent.mkdir(parents=True, exist_ok=True)
    tmp = GZ_OUT.with_suffix('.tmp.gz')
    with gzip.open(tmp, 'wt', compresslevel=9) as f:
        json.dump(candles, f, separators=(',',':'))
    tmp.rename(GZ_OUT)


def show(candles):
    import datetime
    prices = [c['c'] for c in candles]
    vols   = [c['vol'] for c in candles]
    active = [c for c in candles if c['vol'] > 0]
    print(f"\n1m candles  : {len(candles)}")
    print(f"Active bars : {len(active)}")
    print(f"Price range : ${min(prices):,.2f} - ${max(prices):,.2f}"
          f"  (${max(prices)-min(prices):.2f})")
    print(f"Total vol   : {sum(vols):.4f} BTC")
    print(f"eff_usd     : median=${float(np.median([c['eff_usd'] for c in candles])):,.0f}"
          f"  max=${max(c['eff_usd'] for c in candles):,.0f}")
    print(f"\n--- TOP 5 VOLUME BARS ---")
    for c in sorted(active, key=lambda x: x['vol'], reverse=True)[:5]:
        ts = datetime.datetime.utcfromtimestamp(c['ts']//1000).strftime('%H:%M')
        print(f"  {ts}  ${c['c']:,.2f}  vol={c['vol']:.4f} BTC"
              f"  ob={c['ob']:.2f}  eff_usd=${c['eff_usd']:,.0f}")


if __name__ == '__main__':
    print("Fetching BTCUSDT data from data/raw...", flush=True)
    subprocess.run(['git','fetch','origin','data/raw'], capture_output=True)
    lines   = fetch()
    print(f"Raw lines: {len(lines)}")
    print("Sessions detected:")
    candles = build(lines)
    save(candles)
    show(candles)
    print(f"\nSaved -> {GZ_OUT.name}")
