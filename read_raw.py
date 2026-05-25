#!/usr/bin/env python3
"""
read_raw.py — fetch and print raw data from data/raw branch.
Usage: python read_raw.py
"""
import gzip, json, os, subprocess, sys
from pathlib import Path

REPO   = Path(__file__).resolve().parent
os.chdir(REPO)
GZ     = REPO / "data" / "raw" / "BTCUSDC_LIVE.jsonl.gz"

def git(*a):
    return subprocess.run(['git','-C',str(REPO)]+list(a), capture_output=True, text=True)

# Fetch latest
print("Fetching data/raw...", flush=True)
git('fetch','origin','data/raw')

# Read from remote ref directly
r = subprocess.run(
    ['git','-C',str(REPO),'show','origin/data/raw:data/raw/BTCUSDC_LIVE.jsonl.gz'],
    capture_output=True)

if not r.stdout:
    print("No data on data/raw yet."); sys.exit(1)

lines = gzip.decompress(r.stdout).decode().strip().split('\n')
trades = [json.loads(l) for l in lines if l.startswith('["T"')]
depths = [json.loads(l) for l in lines if l.startswith('["D"')]

print(f"\nTotal records : {len(lines)}")
print(f"Trades        : {len(trades)}")
print(f"Depth snapshots: {len(depths)}")

if trades:
    print("\n--- TRADES (last 5) ---")
    for t in trades[-5:]:
        _, ts, p_c, q_u, side = t
        price = p_c / 100
        qty   = q_u / 10000
        s     = "BUY" if side == 1 else "SELL"
        print(f"  {s:4s}  ${price:,.2f}  qty={qty:.4f}  ts={ts}")

if depths:
    print("\n--- ORDER BOOK (latest snapshot) ---")
    _, ts, bids, asks = depths[-1]
    print(f"  snapshot ts={ts}")
    print(f"  TOP 5 BIDS:")
    for p_c, q_u in bids[:5]:
        print(f"    bid ${p_c/100:,.2f}  qty={q_u/10000:.4f}")
    print(f"  TOP 5 ASKS:")
    for p_c, q_u in asks[:5]:
        print(f"    ask ${p_c/100:,.2f}  qty={q_u/10000:.4f}")
