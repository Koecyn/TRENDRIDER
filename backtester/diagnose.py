#!/usr/bin/env python3
"""
diagnose.py — read-only entry condition checker
Run:  python diagnose.py
Paste the output to Claude so he can see what's blocking trades.
No orders placed. Uses same .env keys.
"""
import os, sys, time, math
import numpy as np
from dotenv import load_dotenv
from binance.client import Client

load_dotenv()
api_key    = os.getenv('BINANCE_API_KEY')
api_secret = os.getenv('BINANCE_SECRET_KEY')
if not api_key or not api_secret:
    print("ERROR: BINANCE_API_KEY / BINANCE_SECRET_KEY not in .env"); sys.exit(1)

client = Client(api_key, api_secret, tld='us')

BB_WINDOW=15; BB_NSTD=1.5; RSI_PERIOD=14; MOM_FAST=3; MOM_SLOW=12
SMA_WINDOW=60; BB_ENTRY_PCT=0.50; MOM5M_THRESH=-0.0001
G='\033[92m'; R='\033[91m'; Y='\033[93m'; Z='\033[0m'; B='\033[1m'

def _ema(arr, span):
    alpha = 2.0/(span+1); out = np.full(len(arr), np.nan)
    fv = np.where(~np.isnan(arr))[0]
    if not len(fv): return out
    out[fv[0]] = arr[fv[0]]
    for i in range(fv[0]+1, len(arr)):
        v = arr[i] if not np.isnan(arr[i]) else out[i-1]
        out[i] = alpha*v + (1-alpha)*out[i-1]
    return out

def calc_rsi(c):
    d=np.diff(c, prepend=c[0])
    return 100.-100./(1.+_ema(np.maximum(d,0),2*RSI_PERIOD-1)/(_ema(np.maximum(-d,0),2*RSI_PERIOD-1)+1e-12))

def calc_bb(c):
    n=len(c); m=np.full(n,np.nan); s=np.full(n,np.nan)
    for i in range(BB_WINDOW-1,n):
        sl=c[i-BB_WINDOW+1:i+1]; m[i]=sl.mean(); s[i]=sl.std()
    lo=m-BB_NSTD*s; hi=m+BB_NSTD*s
    return m,lo,hi,s

def calc_mom(c):
    mom=(_ema(c,MOM_FAST)-_ema(c,MOM_SLOW))/(c+1e-12)
    return _ema(np.gradient(mom),2)

def macro_1m(c1m):
    if len(c1m)<72: return False, c1m[-1] if len(c1m) else 0, 0
    sma=c1m[-60:].mean(); v=(_ema(c1m,3)[-1]-_ema(c1m,12)[-1])/(c1m[-1]+1e-12)
    return bool(c1m[-1]>sma and v>MOM5M_THRESH), sma, v

def pf(ok, val):
    return f"{G}{val} ✓{Z}" if ok else f"{R}{val} ✗{Z}"

print(f"\n{B}Fetching 200 1-min klines…{Z}")
klines = client.get_klines(symbol='BTCUSDT',
                            interval=Client.KLINE_INTERVAL_1MINUTE, limit=200)
c1m = np.array([float(k[4]) for k in klines])
print(f"Got {len(c1m)} bars. Price range ${c1m.min():.2f}–${c1m.max():.2f}\n")

try:
    while True:
        price = float(client.get_symbol_ticker(symbol='BTCUSDT')['price'])
        c     = np.append(c1m, price)

        mid, lo, hi, std = calc_bb(c)
        rsi  = calc_rsi(c)[-1]
        mom  = calc_mom(c)[-1]
        bm   = mid[-1]; bl = lo[-1]; bh = hi[-1]; bs = std[-1]
        cpct = (price - bl) / (bh - bl + 1e-12) * 100
        mac, sma60, mom5m = macro_1m(c1m)

        bb_ok  = cpct < BB_ENTRY_PCT * 100
        mac_ok = mac
        all_ok = bb_ok and mac_ok

        sys.stdout.write('\033[2J\033[H'); sys.stdout.flush()
        print(f"  {B}ENTRY CONDITION CHECKER{Z}  —  read-only")
        print(f"  {'═'*52}")
        print(f"  Price      ${price:,.2f}")
        print(f"  BB lower   ${bl:,.2f}  mid ${bm:,.2f}  upper ${bh:,.2f}")
        print(f"  BB width   ${bh-bl:,.2f}   std ${std[-1]:,.2f}")
        print(f"  SMA60(1m)  ${sma60:,.2f}   mom5m {mom5m:+.6f}")
        print()
        print(f"  {'CONDITION':<25} {'VALUE':>8}  {'NEED':>8}  STATUS")
        print(f"  {'─'*52}")
        print(f"  {'BB zone (price vs midline)':<25} {cpct:>7.1f}%  {'< 50%':>8}  {pf(bb_ok,'✓' if bb_ok else '✗')}")
        print(f"  {'Macro uptrend (1m)':<25} {'YES' if mac else 'NO':>8}  {'YES':>8}  {pf(mac_ok,'✓' if mac_ok else '✗')}")
        print(f"  {'─'*52}")

        if all_ok:
            print(f"  {G}{B}★ SIGNAL — would enter NOW{Z}")
        else:
            fails = []
            if not bb_ok:  fails.append(f"price above midline ({cpct:.0f}% — need <50%)")
            if not mac_ok: fails.append(f"not uptrend (price {'above' if price>sma60 else 'BELOW'} SMA60 by ${abs(price-sma60):.2f})")
            print(f"  {R}Blocking: {' | '.join(fails)}{Z}")

        print(f"\n  1-min bars loaded: {len(c1m)}  (need 72 for macro)")
        print(f"  Ctrl+C to stop  —  refreshing every 5s")
        time.sleep(5)

except KeyboardInterrupt:
    print("\nDone.")
