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
from dotenv import load_dotenv
from binance.client import Client
from binance.exceptions import BinanceAPIException

# ── Configuration — matched to backtest (Sharpe 2.15, 57.5% WR) ──────────────
SYMBOL        = "BTCUSDT"
BAR_S         = 5          # 5-second bars
LIVE_WARMUP   = 5          # 5 live 5s bars (~25s) — history pre-loaded, just sync price
EQUITY_PCT    = 0.95       # deploy 95% of free USDT per trade

BB_WINDOW     = 60         # 60 × 5s = 5-min Bollinger window (needs range for variance)
BB_NSTD       = 1.5        # 1.5σ bands
RSI_PERIOD    = 14         # 14 × 5s = 70-second RSI
MOM_FAST      = 3
MOM_SLOW      = 12
SMA_WINDOW    = 60         # 60 × 5s = 5-min SMA for macro uptrend gate

RSI_EXIT      = 65.0       # RSI > 65 → extended, take profit
BB_ENTRY_PCT  = 0.50       # enter when price below BB midline in uptrend
MAX_HOLD_S    = 30         # time-stop: sell after 30 seconds regardless
MOM5M_THRESH  = -0.0001    # macro_up gate: 5-min momentum must exceed this
ORDER_TIMEOUT = 45         # strike window: 45s to get maker fill before cancelling

# Regime-aware ATR stop multipliers — tight in downtrend, wider in uptrend
STOP_ATR_MULT = {
    'UPTREND':   0.30,
    'RANGING':   0.20,
    'COMPRESS':  0.15,
    'DOWNTREND': 0.08,   # $4 stop on $50 ATR — tiny, preserve capital
    'UNKNOWN':   0.15,
}
STOP_MIN_USD = 2.0    # never tighter than $2
STOP_MAX_USD = 15.0   # never wider than $15

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

def calc_atr(highs, lows, closes, period=14):
    """ATR + downward ratio: how much of each bar's range is bearish movement."""
    h = np.array(highs); l = np.array(lows); c = np.array(closes)
    prev_c = np.roll(c, 1); prev_c[0] = c[0]
    tr  = np.maximum(h - l, np.maximum(np.abs(h - prev_c), np.abs(l - prev_c)))
    atr = float(_ema(tr, period)[-1])
    # Down component: how much of each bar moved below the previous close
    down = np.maximum(prev_c - l, 0)
    up   = np.maximum(h - prev_c, 0)
    total = down + up + 1e-12
    down_ratio = float(_ema(down / total, period)[-1])   # 0=all up, 1=all down
    return atr, down_ratio

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

# ── Regime detection ─────────────────────────────────────────────────────────
# Entry bb_pct threshold per regime (None = no longs)
REGIME_ENTRY = {
    'UPTREND':   0.50,   # any pullback below midline
    'RANGING':   0.28,   # near lower band
    'COMPRESS':  0.20,   # only extreme lower band — breakout imminent
    'DOWNTREND': None,   # no long entries
    'UNKNOWN':   0.28,
}
# Exit bb_pct threshold per regime
REGIME_EXIT = {
    'UPTREND':   0.82,   # near upper band — ride the bounce
    'RANGING':   0.50,   # midline
    'COMPRESS':  0.55,   # just past midline
    'DOWNTREND': 0.50,
    'UNKNOWN':   0.50,
}
REGIME_COLOR = {         # for dashboard
    'UPTREND': '\033[92m', 'DOWNTREND': '\033[91m',
    'RANGING': '\033[93m', 'COMPRESS':  '\033[96m', 'UNKNOWN': '\033[2m',
}

def classify_regime(closes_1m):
    """
    Detect regime from 1-min closes using EMA alignment + swing structure.
    Returns: 'UPTREND' | 'DOWNTREND' | 'RANGING' | 'COMPRESS' | 'UNKNOWN'
    """
    c = np.array(closes_1m)
    if len(c) < 30:
        return 'UNKNOWN'
    e20   = _ema(c, 20)
    e60   = _ema(c, min(60, len(c)))
    slope = (e20[-1] - e20[-6]) / (e20[-6] + 1e-12)
    # Swing structure over last 20 bars
    seg       = c[-20:]
    h1, l1    = seg[:10].max(), seg[:10].min()
    h2, l2    = seg[10:].max(), seg[10:].min()
    hh = h2 > h1 * 1.0002    # higher highs
    hl = l2 > l1 * 1.0002    # higher lows
    lh = h2 < h1 * 0.9998    # lower highs
    ll = l2 < l1 * 0.9998    # lower lows
    above = c[-1] > e20[-1]
    below = c[-1] < e20[-1]
    if lh and hl:
        return 'COMPRESS'                     # triangle: lower highs + higher lows
    if above and slope > 0.00005 and (hh or hl):
        return 'UPTREND'
    if below and slope < -0.00005 and (ll or lh):
        return 'DOWNTREND'
    return 'RANGING'

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
