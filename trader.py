#!/usr/bin/env python3
"""trader.py — Binance.US mean-reversion bot.  python trader.py"""
import os, sys, time, math
from datetime import datetime
from collections import deque
import numpy as np
from dotenv import load_dotenv
from binance.client import Client
from binance.exceptions import BinanceAPIException

# ── Config ────────────────────────────────────────────────────────────────────
SYMBOL     = "BTCUSDT"
TICK_S     = 0.25       # 250 ms poll
EQUITY_PCT = 0.95       # fraction of free USDT per trade

BB_WINDOW  = 20         # bars (1-min closes → 20-min bands)
BB_NSTD    = 2.0        # standard deviations
RSI_PERIOD = 14
ENTRY_PCT  = 0.35       # enter when price in bottom 35 % of BB range
EXIT_PCT   = 0.72       # exit when price reaches top 28 % of BB range
MAX_HOLD_S = 90         # hard time-stop: sell after 90 seconds
TRAIL_PCT  = 0.0015     # 0.15 % trailing stop below peak

TAKER_FEE  = 0.00020    # 0.02 % market orders on Binance.US
W          = 66

G='\033[92m'; R='\033[91m'; Y='\033[93m'; C='\033[96m'; B='\033[1m'; Z='\033[0m'
SP = '|/-\\'

# ── Indicators ────────────────────────────────────────────────────────────────
def _ema(arr, span):
    a = 2.0 / (span + 1)
    out = np.full(len(arr), np.nan)
    fv = np.where(~np.isnan(arr))[0]
    if not len(fv): return out
    out[fv[0]] = arr[fv[0]]
    for i in range(fv[0]+1, len(arr)):
        v = arr[i] if not np.isnan(arr[i]) else out[i-1]
        out[i] = a*v + (1-a)*out[i-1]
    return out

def calc_rsi(c, period=14):
    d = np.diff(c, prepend=c[0])
    return 100 - 100/(1 + _ema(np.maximum(d,0), 2*period-1)
                         /(_ema(np.maximum(-d,0), 2*period-1)+1e-12))

def calc_bb(c, window=20, n=2.0):
    sz = len(c)
    mid = np.full(sz, np.nan); std = np.full(sz, np.nan)
    for i in range(window-1, sz):
        s = c[i-window+1:i+1]; mid[i]=s.mean(); std[i]=s.std()
    lo = mid - n*std; hi = mid + n*std
    pct = (c - lo) / (hi - lo + 1e-12)
    return mid, lo, hi, pct, std

# ── Exchange helpers ───────────────────────────────────────────────────────────
def get_filters(client, sym):
    f = {'min_notional': 1.0, 'step': 0.00001, 'tick': 0.01, 'min_qty': 0.00001}
    for flt in client.get_symbol_info(sym)['filters']:
        ft = flt['filterType']
        if ft == 'LOT_SIZE':
            f['step'] = float(flt['stepSize']); f['min_qty'] = float(flt['minQty'])
        elif ft == 'PRICE_FILTER':
            f['tick'] = float(flt['tickSize'])
        elif ft in ('MIN_NOTIONAL', 'NOTIONAL'):
            f['min_notional'] = float(flt.get('minNotional', 1.0))
    return f

def floor_qty(qty, step):
    p = max(0, round(-math.log10(step)))
    return math.floor(qty * 10**p) / 10**p

def fmt_qty(qty, step):
    p = max(0, round(-math.log10(step)))
    return f"{floor_qty(qty,step):.{p}f}"

def balances(client):
    b = {x['asset']: float(x['free']) for x in client.get_account()['balances']}
    return b.get('BTC', 0.0), b.get('USDT', 0.0)

def market_buy(client, sym, qty, step):
    o = client.order_market_buy(symbol=sym, quantity=fmt_qty(qty, step))
    fills = o.get('fills', [])
    if fills:
        tq = sum(float(f['qty']) for f in fills)
        return sum(float(f['price'])*float(f['qty']) for f in fills)/tq, tq
    return None, None

def market_sell(client, sym, qty, step):
    o = client.order_market_sell(symbol=sym, quantity=fmt_qty(qty, step))
    fills = o.get('fills', [])
    if fills:
        tq = sum(float(f['qty']) for f in fills)
        return sum(float(f['price'])*float(f['qty']) for f in fills)/tq, tq
    return None, None

# ── Main ──────────────────────────────────────────────────────────────────────
def main():
    load_dotenv()
    key = os.getenv('BINANCE_API_KEY'); sec = os.getenv('BINANCE_SECRET_KEY')
    if not key or not sec:
        sys.exit("ERROR: BINANCE_API_KEY / BINANCE_SECRET_KEY missing from .env")

    client = Client(key, sec, tld='us')
    flt = get_filters(client, SYMBOL)
    step = flt['step']; tick = flt['tick']
    min_qty = flt['min_qty']; min_notional = flt['min_notional']

    print(f"Connected  step={step}  min_notional=${min_notional}")
    print("Loading 200 1-min klines…")
    klines = client.get_klines(symbol=SYMBOL, interval=Client.KLINE_INTERVAL_1MINUTE, limit=200)
    history = deque([float(k[4]) for k in klines], maxlen=1000)

    btc, usdt = balances(client)
    print(f"Balance  ${usdt:.4f} USDT  {btc:.8f} BTC")
    time.sleep(1)

    pos    = None   # {entry, qty, peak, time}
    trades = []
    log    = deque(maxlen=5000)
    spin   = 0
    bar_prices = []    # intra-5s price samples for bar close
    last_bar   = time.time()

    def _log(msg):
        log.append(f"{datetime.now().strftime('%H:%M:%S.%f')[:-3]}  {msg}")

    try:
        while True:
            t0 = time.time()

            # ── Fetch price ───────────────────────────────────────────────
            price = float(client.get_symbol_ticker(symbol=SYMBOL)['price'])
            bar_prices.append(price)

            # ── Close 5-second bar into history ───────────────────────────
            if t0 - last_bar >= 5.0:
                history.append(float(np.mean(bar_prices)))
                bar_prices = []
                last_bar = t0

            # ── Compute indicators every tick from history + live price ───
            c = np.append(np.array(history), price)
            ready = len(c) >= max(BB_WINDOW, RSI_PERIOD)
            ind = {}
            if ready:
                mid, lo, hi, pct, std = calc_bb(c, BB_WINDOW, BB_NSTD)
                ind = {
                    'mid': float(mid[-1]), 'lo': float(lo[-1]),
                    'hi':  float(hi[-1]),  'pct': float(pct[-1]),
                    'std': float(std[-1]), 'rsi': float(calc_rsi(c, RSI_PERIOD)[-1]),
                    'width': float(hi[-1]-lo[-1]),
                }

            # ── Dashboard ─────────────────────────────────────────────────
            sys.stdout.write('\033[2J\033[H'); sys.stdout.flush()
            spin = (spin+1) % 4
            total = usdt + btc*price
            print(f"{C}{'═'*W}{Z}")
            print(f"  {B}BTCUSDT{Z}  ${price:,.2f}  {SP[spin]}  "
                  f"{datetime.now().strftime('%H:%M:%S')}")
            print(f"  Wallet  ${usdt:.4f} USDT  {btc:.8f} BTC  "
                  f"Total ${total:.4f}")
            print(f"{C}{'─'*W}{Z}")

            if ind:
                bc = G if ind['pct'] < ENTRY_PCT else (R if ind['pct'] > EXIT_PCT else Y)
                rc = G if ind['rsi'] < 40 else (R if ind['rsi'] > 65 else Z)
                print(f"  BB  ${ind['lo']:,.2f} │ ${ind['mid']:,.2f} │ ${ind['hi']:,.2f}"
                      f"  width ${ind['width']:,.2f}")
                print(f"  BB% {bc}{ind['pct']*100:.1f}%{Z}  "
                      f"RSI {rc}{ind['rsi']:.1f}{Z}  "
                      f"bars {len(c)}")
            else:
                print(f"  warming up… {len(c)}/{BB_WINDOW} bars needed")

            if pos:
                held = t0 - pos['time']
                unr  = (price - pos['entry']) * pos['qty']
                trail_stop = pos['peak'] * (1 - TRAIL_PCT)
                col  = G if unr >= 0 else R
                print(f"  {col}▶ IN POSITION  entry ${pos['entry']:,.2f}  "
                      f"qty {pos['qty']}  "
                      f"P&L {'+' if unr>=0 else ''}{unr:.5f}  "
                      f"held {held:.0f}s{Z}")
                print(f"  trail ${trail_stop:,.2f}  peak ${pos['peak']:,.2f}")
            elif ind:
                if ind['pct'] < ENTRY_PCT:
                    print(f"  {G}★ ENTRY ZONE  BB {ind['pct']*100:.1f}% < {ENTRY_PCT*100:.0f}%{Z}")
                else:
                    print(f"  waiting  BB {ind['pct']*100:.1f}%  need < {ENTRY_PCT*100:.0f}%")

            wins = sum(1 for t in trades if t['pnl'] > 0)
            net  = sum(t['pnl'] - t['fee'] for t in trades)
            nc   = G if net >= 0 else R
            print(f"  Trades {len(trades)}  Wins {wins}  "
                  f"Net {nc}{'+' if net>=0 else ''}{net:.5f}{Z}")
            if trades:
                t = trades[-1]
                nt = t['pnl'] - t['fee']
                print(f"  Last: {t['time']}  {t['reason']}  "
                      f"${t['entry']:,.2f}→${t['exit']:,.2f}  "
                      f"{'+' if nt>=0 else ''}{nt:.5f}")
            print(f"{C}{'─'*W}{Z}  Ctrl+C → scroll log")

            # ── Trading logic ─────────────────────────────────────────────
            if not ind:
                time.sleep(max(0, TICK_S - (time.time()-t0))); continue

            # Update trailing stop
            if pos:
                pos['peak'] = max(pos['peak'], price)

            if pos:
                held       = t0 - pos['time']
                trail_stop = pos['peak'] * (1 - TRAIL_PCT)
                reason     = None
                if price <= trail_stop:
                    reason = 'TRAIL' if price >= pos['entry'] else 'STOP'
                elif held >= MAX_HOLD_S:
                    reason = 'TIME'
                elif ind['pct'] > EXIT_PCT:
                    reason = 'TARGET'

                if reason:
                    _log(f"→ EXIT {reason}  entry ${pos['entry']:,.2f}  "
                         f"price ${price:,.2f}  held {held:.0f}s")
                    try:
                        ep, eq = market_sell(client, SYMBOL, pos['qty'], step)
                        if ep is None: ep = price; eq = pos['qty']
                        fee = ep * eq * TAKER_FEE
                        pnl = (ep - pos['entry']) * eq
                        trades.append({'pnl': pnl, 'fee': fee, 'reason': reason,
                                       'entry': pos['entry'], 'exit': ep,
                                       'time': datetime.now().strftime('%H:%M:%S')})
                        _log(f"  ↳ sold ${ep:,.2f}  "
                             f"PnL {'+' if pnl>=0 else ''}{pnl:.5f}  fee {fee:.5f}")
                        pos = None
                        btc, usdt = balances(client)
                    except BinanceAPIException as e:
                        _log(f"SELL ERR {e.status_code}: {e.message}")

            elif ind['pct'] < ENTRY_PCT and ind['rsi'] < 65:
                qty      = floor_qty(usdt * EQUITY_PCT / price, step)
                notional = qty * price
                _log(f"★ SIGNAL  ${price:,.2f}  BB {ind['pct']*100:.1f}%  "
                     f"RSI {ind['rsi']:.1f}  qty {qty}  notional ${notional:.4f}")
                if qty >= min_qty and notional >= min_notional:
                    try:
                        ep, eq = market_buy(client, SYMBOL, qty, step)
                        if ep is None: ep = price; eq = qty
                        pos = {'entry': ep, 'qty': eq, 'peak': ep, 'time': time.time()}
                        btc, usdt = balances(client)
                        _log(f"✓ BOUGHT ${ep:,.2f}  qty {eq}")
                    except BinanceAPIException as e:
                        _log(f"BUY ERR {e.status_code}: {e.message}")
                else:
                    _log(f"✗ INSUF  qty {qty}  notional ${notional:.4f}  "
                         f"min_qty {min_qty}  min_notional ${min_notional}")

            time.sleep(max(0, TICK_S - (time.time()-t0)))

    except KeyboardInterrupt:
        pass

    # ── Scroll log dump ───────────────────────────────────────────────────────
    wins = sum(1 for t in trades if t['pnl'] > 0)
    net  = sum(t['pnl'] - t['fee'] for t in trades)
    print(f"\n{C}{'═'*W}{Z}")
    print(f"  {B}SESSION LOG{Z}  {len(log)} entries  "
          f"{len(trades)} trades  {wins} wins  "
          f"net {'+' if net>=0 else ''}{net:.5f}")
    print(f"{C}{'─'*W}{Z}")
    for line in log:
        if '★' in line:   print(f"{G}  {line}{Z}")
        elif '✓' in line: print(f"{C}  {line}{Z}")
        elif 'ERR' in line or '✗' in line: print(f"{R}  {line}{Z}")
        elif '→ EXIT' in line or '↳' in line:
            print(f"{'  ' + G + line + Z if '+' in line else '  ' + R + line + Z}")
        else: print(f"  {line}")
    print(f"{C}{'═'*W}{Z}")

if __name__ == '__main__':
    main()
