#!/usr/bin/env python3
# live_trader_p1.py  —  Part 1 of 2
# Usage: cat live_trader_p1.py live_trader_p2.py > live_trader.py
#        python live_trader.py

# ── Imports ───────────────────────────────────────────────────────────────────
import os
import sys
import time
import math
from datetime import datetime
from collections import deque
import numpy as np
from binance.client import Client
from binance.exceptions import BinanceAPIException

# ── Configuration — matched to backtest (Sharpe 2.15, 57.5% WR) ──────────────
SYMBOL        = "BTCUSDT"
BAR_S         = 5          # 5-second bars
LIVE_WARMUP   = 15         # live 5s bars before trading (~75 seconds)
EQUITY_PCT    = 0.95       # deploy 95% of free USDT per trade

BB_WINDOW     = 15         # 15 × 5s = 75-second Bollinger window
BB_NSTD       = 1.5        # 1.5σ bands → 3:1 R:R with 0.5σ stop
RSI_PERIOD    = 14
MOM_FAST      = 3
MOM_SLOW      = 12
SMA_WINDOW    = 60         # 60 × 5s = 5-min SMA for macro uptrend gate

RSI_ENTRY     = 42.0       # RSI < 42 → oversold, buy signal
RSI_EXIT      = 58.0       # RSI > 58 → overbought, sell signal
BB_ENTRY_PCT  = 0.20       # enter when price in bottom 20% of BB range
BB_STOP_MULT  = 0.5        # stop = bb_lower − 0.5 × bb_std
MAX_HOLD_S    = 30         # time-stop: sell after 30 seconds regardless
MOM5M_THRESH  = -0.0001    # macro_up gate: 5-min momentum must exceed this
ORDER_TIMEOUT = 8          # seconds to wait for maker fill before cancelling

# Binance.US fees (your account tier)
MAKER_FEE     = 0.0        # 0%    — post-only limit orders
TAKER_FEE     = 0.00002    # 0.002% — market / IOC orders

# ── Indicators — causal, zero lookahead ───────────────────────────────────────
def _ema(arr, span):
    alpha = 2.0 / (span + 1)
    out   = np.full(len(arr), np.nan)
    fv    = np.where(~np.isnan(arr))[0]
    if not len(fv):
        return out
    out[fv[0]] = arr[fv[0]]
    for i in range(fv[0] + 1, len(arr)):
        v      = arr[i] if not np.isnan(arr[i]) else out[i - 1]
        out[i] = alpha * v + (1 - alpha) * out[i - 1]
    return out

def calc_rsi(close, period=14):
    d  = np.diff(close, prepend=close[0])
    ag = _ema(np.maximum(d,  0), 2 * period - 1)
    al = _ema(np.maximum(-d, 0), 2 * period - 1)
    return 100.0 - 100.0 / (1.0 + ag / (al + 1e-12))

def calc_bb(close, window=15, n_std=1.5):
    n = len(close)
    mid = np.full(n, np.nan)
    std = np.full(n, np.nan)
    for i in range(window - 1, n):
        s      = close[i - window + 1: i + 1]
        mid[i] = s.mean()
        std[i] = s.std()
    lo    = mid - n_std * std
    hi    = mid + n_std * std
    pct_b = (close - lo) / (hi - lo + 1e-12)
    return mid, lo, hi, pct_b, std

def calc_mom_slope(close, fast=3, slow=12, smooth=2):
    mom = (_ema(close, fast) - _ema(close, slow)) / (close + 1e-12)
    return _ema(np.gradient(mom), smooth)

def calc_macro_up(closes_5s, sma_win=60):
    """price > 5-min SMA AND 5-min momentum not bearish — matches strategy.py"""
    arr = np.array(closes_5s)
    if len(arr) < sma_win + 12:
        return False
    sma   = arr[-sma_win:].mean()
    n5m   = len(arr) // 60
    if n5m < 12:
        return False
    c5m   = np.array([arr[(i + 1) * 60 - 1] for i in range(n5m)])
    mom5m = (_ema(c5m, 3)[-1] - _ema(c5m, 12)[-1]) / (c5m[-1] + 1e-12)
    return bool(arr[-1] > sma and mom5m > MOM5M_THRESH)

# ── Exchange helpers — Binance.US ─────────────────────────────────────────────
def get_filters(client, symbol):
    info = client.get_symbol_info(symbol)
    f    = {'min_notional': 10.0}
    for flt in info['filters']:
        ft = flt['filterType']
        if ft == 'LOT_SIZE':
            f['step_size'] = float(flt['stepSize'])
            f['min_qty']   = float(flt['minQty'])
        elif ft == 'PRICE_FILTER':
            f['tick_size'] = float(flt['tickSize'])
        elif ft in ('MIN_NOTIONAL', 'NOTIONAL'):
            f['min_notional'] = float(flt.get('minNotional', 10))
    return f

def floor_qty(qty, step):
    p = max(0, round(-math.log10(step)))
    return math.floor(qty * 10 ** p) / 10 ** p

def fmt_qty(qty, step):
    p = max(0, round(-math.log10(step)))
    return f"{floor_qty(qty, step):.{p}f}"

def round_px(price, tick):
    p = max(0, round(-math.log10(tick)))
    return round(price, p)

def fmt_px(price, tick):
    p = max(0, round(-math.log10(tick)))
    return f"{round_px(price, tick):.{p}f}"

def get_balances(client, base, quote='USDT'):
    bal = {b['asset']: float(b['free']) for b in client.get_account()['balances']}
    return bal.get(base, 0.0), bal.get(quote, 0.0)

def best_bid_ask(client, symbol):
    bk = client.get_order_book(symbol=symbol, limit=5)
    return float(bk['bids'][0][0]), float(bk['asks'][0][0])
