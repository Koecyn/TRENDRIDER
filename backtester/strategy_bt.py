#!/usr/bin/env python3
"""
strategy_bt.py — TRENDRIDER v7 wrapped for backtesting.py

Usage:
  python strategy_bt.py                        # last 30 days, Binance.US
  python strategy_bt.py --days 90
  python strategy_bt.py --start 2026-01-01
  python strategy_bt.py --csv data.csv
  python strategy_bt.py --csv data.csv --optimize
  python strategy_bt.py --csv data.csv --save-csv data.csv   # fetch + cache
"""

import os, sys, argparse
from pathlib import Path
import numpy as np
import pandas as pd
from backtesting import Backtest, Strategy

try:
    from binance.client import Client
    from dotenv import load_dotenv
    load_dotenv(Path(__file__).parent.parent / ".env")
    HAS_BINANCE = True
except ImportError:
    HAS_BINANCE = False

# ── Parameters (keep in sync with live_trader.py) ─────────────────────────────
P = {
    'emaFast':       5,
    'emaSlow':       13,
    'emaTrend':      50,
    'rsiOB':         80,
    'rsiOS':         28,
    'volMin':        0.0,
    'atrStop':       1.9,
    'atrTp':         2.8,
    'partialAt':     1.0,
    'trailAtr':      0.65,
    'trailActivate': 0.25,
    'maxHoldBars':   25,
    'adxMin':        18,
}

# ── Indicator functions (identical to live_trader.py) ─────────────────────────
def calc_ema(arr, period):
    out = np.zeros(len(arr))
    if len(arr) < period:
        return out
    k = 2 / (period + 1)
    out[period - 1] = np.mean(arr[:period])
    for i in range(period, len(arr)):
        out[i] = arr[i] * k + out[i-1] * (1 - k)
    return out

def calc_rsi(closes, period=14):
    out = np.full(len(closes), 50.0)
    if len(closes) < period + 1:
        return out
    deltas = np.diff(closes)
    gains  = np.where(deltas > 0, deltas, 0.0)
    losses = np.where(deltas < 0, -deltas, 0.0)
    ag, al = np.mean(gains[:period]), np.mean(losses[:period])
    for i in range(period, len(deltas)):
        ag = (ag * (period - 1) + gains[i]) / period
        al = (al * (period - 1) + losses[i]) / period
        rs = ag / al if al > 0 else float('inf')
        out[i + 1] = 100 - 100 / (1 + rs)
    return out

def calc_atr(high, low, close, period=14):
    n   = len(close)
    out = np.zeros(n)
    if n < 2:
        return out
    trs = np.array([max(high[i]-low[i], abs(high[i]-close[i-1]), abs(low[i]-close[i-1]))
                    for i in range(1, n)])
    if len(trs) < period:
        return out
    out[period] = np.mean(trs[:period])
    for i in range(period, len(trs)):
        out[i+1] = (out[i] * (period-1) + trs[i]) / period
    return out

def calc_adx(high, low, close, period=14):
    n   = len(close)
    out = np.zeros(n)
    if n < period * 2:
        return out
    pdm = np.zeros(n); mdm = np.zeros(n); tr = np.zeros(n)
    for i in range(1, n):
        up = high[i]-high[i-1]; dn = low[i-1]-low[i]
        pdm[i] = up if up > dn and up > 0 else 0
        mdm[i] = dn if dn > up and dn > 0 else 0
        tr[i]  = max(high[i]-low[i], abs(high[i]-close[i-1]), abs(low[i]-close[i-1]))
    a14 = np.zeros(n); p14 = np.zeros(n); m14 = np.zeros(n)
    a14[period] = np.sum(tr[1:period+1])
    p14[period] = np.sum(pdm[1:period+1])
    m14[period] = np.sum(mdm[1:period+1])
    for i in range(period+1, n):
        a14[i] = a14[i-1] - a14[i-1]/period + tr[i]
        p14[i] = p14[i-1] - p14[i-1]/period + pdm[i]
        m14[i] = m14[i-1] - m14[i-1]/period + mdm[i]
    for i in range(period, n):
        if a14[i] > 0:
            pdi = 100*p14[i]/a14[i]; mdi = 100*m14[i]/a14[i]
            dx  = 100*abs(pdi-mdi)/(pdi+mdi) if (pdi+mdi) > 0 else 0
            out[i] = dx if i == period else (out[i-1]*(period-1)+dx)/period
    return out

def calc_vol_ratio(volumes, period=10):
    n   = len(volumes)
    out = np.zeros(n)
    for i in range(period, n):
        avg    = np.mean(volumes[i-period:i])
        out[i] = volumes[i] / avg if avg > 0 else 0.0
    return out

# ── Strategy ──────────────────────────────────────────────────────────────────
class TrendRider(Strategy):
    # Tunable — backtesting.py optimizer will sweep these
    adxMin       = P['adxMin']
    rsiOB        = P['rsiOB']
    atrStop      = P['atrStop']
    atrTp        = P['atrTp']
    partialAt    = P['partialAt']
    trailAtr     = P['trailAtr']
    trailActivate = P['trailActivate']
    maxHoldBars  = P['maxHoldBars']

    def init(self):
        c = np.array(self.data.Close)
        h = np.array(self.data.High)
        l = np.array(self.data.Low)
        v = np.array(self.data.Volume)

        self.ema5  = self.I(calc_ema, c, P['emaFast'],  name='EMA5')
        self.ema13 = self.I(calc_ema, c, P['emaSlow'],  name='EMA13')
        self.ema50 = self.I(calc_ema, c, P['emaTrend'], name='EMA50')
        self.rsi   = self.I(calc_rsi, c,                name='RSI')
        self.atr   = self.I(calc_atr, h, l, c,          name='ATR')
        self.adx   = self.I(calc_adx, h, l, c,          name='ADX')
        self.vol   = self.I(calc_vol_ratio, v,           name='VolRatio')

        # Per-trade state (manual, since backtesting.py doesn't do partials/trail)
        self._stop          = 0.0
        self._target        = 0.0
        self._partial_at    = 0.0
        self._partial_taken = False
        self._bars_held     = 0
        self._entry         = 0.0
        self._atr_at_entry  = 0.0

    def next(self):
        price = self.data.Close[-1]
        f,  f1 = self.ema5[-1],  self.ema5[-2]
        s,  s1 = self.ema13[-1], self.ema13[-2]
        tr      = self.ema50[-1]
        r       = self.rsi[-1]
        a       = self.atr[-1]
        dx      = self.adx[-1]
        v       = self.vol[-1]
        lo      = self.data.Low[-1]

        # ── Manage open position ──────────────────────────────────────────────
        if self.position:
            self._bars_held += 1

            # Trailing stop
            riskUnit = abs(self._entry - self._stop)
            pnlR = (price - self._entry) / riskUnit if riskUnit > 0 else 0
            if pnlR >= self.trailActivate:
                trail = price - self._atr_at_entry * self.trailAtr
                self._stop = max(self._stop, trail)

            # Partial exit at 1×ATR
            if not self._partial_taken and price >= self._partial_at:
                self.position.close(0.5)
                self._partial_taken = True

            # Exit conditions
            if price <= self._stop or price >= self._target or self._bars_held >= self.maxHoldBars:
                self.position.close()
            return

        # ── Signal detection ──────────────────────────────────────────────────
        if a <= 0 or dx < self.adxMin:
            return

        setup = None
        if (f > s and f1 <= s1 and price > tr and
                v >= P['volMin'] and r < self.rsiOB):
            setup = 'EMA_CROSS'
        elif (f > s and f1 > s1 and price > tr and
                lo <= f * 1.002 and price > f and
                v >= P['volMin'] and P['rsiOS'] < r < 55):
            setup = 'EMA_PULLBACK'

        if setup:
            self._entry         = price
            self._stop          = price - a * self.atrStop
            self._target        = price + a * self.atrTp
            self._partial_at    = price + a * self.partialAt
            self._partial_taken = False
            self._bars_held     = 0
            self._atr_at_entry  = a
            # size=0.99 means 99% of available equity (fractional, not units)
            self.buy(size=0.99)

# ── Data fetching ─────────────────────────────────────────────────────────────
def fetch_binance(days=30, start=None):
    if not HAS_BINANCE:
        sys.exit("python-binance not installed: pip install python-binance")
    from datetime import datetime, timezone
    # Public endpoint — no API keys needed for historical klines
    client     = Client("", "", tld='us')

    now_ms = int(datetime.now(timezone.utc).timestamp() * 1000)
    if start:
        from datetime import timezone
        start_ms = int(datetime.strptime(start, '%Y-%m-%d')
                       .replace(tzinfo=timezone.utc).timestamp() * 1000)
    else:
        start_ms = now_ms - days * 86400 * 1000

    print(f"Fetching BTCUSDT 1m from Binance.US...")
    raw, cur = [], start_ms
    while cur < now_ms:
        chunk = client.get_klines(symbol='BTCUSDT', interval='1m',
                                  startTime=cur, limit=1000)
        if not chunk:
            break
        raw.extend(chunk)
        cur = chunk[-1][0] + 60000
        print(f"  {len(raw):,} bars...", end='\r', flush=True)
    print(f"\n  {len(raw):,} bars fetched.")
    return raw_to_df(raw)

def raw_to_df(raw):
    from datetime import datetime, timezone
    df = pd.DataFrame(raw, columns=[
        'time','open','high','low','close','volume',
        'close_time','qav','trades','tbav','tbqav','ignore'])
    df['time']   = pd.to_datetime(df['time'], unit='ms', utc=True)
    df = df.set_index('time')
    df = df[['open','high','low','close','volume']].astype(float)
    df.columns  = ['Open','High','Low','Close','Volume']
    return df

def load_csv(path):
    df = pd.read_csv(path, index_col=0, parse_dates=True)
    df.columns = [c.capitalize() for c in df.columns]
    if not isinstance(df.index, pd.DatetimeIndex):
        df.index = pd.to_datetime(df.index, utc=True)
    return df

def save_csv(df, path):
    df.to_csv(path)
    print(f"Saved {len(df):,} bars → {path}")

# ── Entry point ───────────────────────────────────────────────────────────────
def main():
    ap = argparse.ArgumentParser(description='TRENDRIDER v7 — backtesting.py')
    ap.add_argument('--days',     type=int,   default=30)
    ap.add_argument('--start',    type=str,   default=None)
    ap.add_argument('--csv',      type=str,   default=None)
    ap.add_argument('--save-csv', type=str,   default=None)
    ap.add_argument('--balance',  type=float, default=100.0)
    ap.add_argument('--optimize', action='store_true')
    args = ap.parse_args()

    if args.csv:
        df = load_csv(args.csv)
        print(f"Loaded {len(df):,} bars from {args.csv}")
    else:
        df = fetch_binance(days=args.days, start=args.start)
        if args.save_csv:
            save_csv(df, args.save_csv)

    # backtesting.py rounds size to integers: round(fraction * equity / price)
    # BTC at ~$76k with $100 balance: round(0.99 * 100 / 76000) = 0 → all orders cancelled
    # Fix: scale prices so 1 unit ≈ $1, giving ~99 units per trade
    med   = df['Close'].median()
    scale = max(1, round(med / (args.balance / 100)))
    if scale > 1:
        df[['Open', 'High', 'Low', 'Close']] /= scale
        print(f"Price scaled by 1/{scale} (median ${med:,.0f} → ${med/scale:.2f}) — "
              f"~{round(0.99 * args.balance / (med / scale))} units per trade")

    # margin=1.0 = spot only, flat cash, no leverage, no borrowing
    bt = Backtest(df, TrendRider, cash=args.balance,
                  commission=0.0002, margin=1.0, exclusive_orders=True)

    if args.optimize:
        print("Running optimization...")
        stats = bt.optimize(
            adxMin       = range(12, 25, 3),
            rsiOB        = range(72, 88, 4),
            atrStop      = [1.5, 1.9, 2.3],
            atrTp        = [2.0, 2.8, 3.5],
            trailAtr     = [0.5, 0.65, 0.8],
            maximize     = 'Sharpe Ratio',
            constraint   = lambda p: p.atrTp > p.atrStop,
            return_heatmap=False,
        )
        print("\n── Optimal parameters ──")
        print(stats._strategy)
    else:
        stats = bt.run()
        print(stats)
        bt.plot(filename='trendrider_backtest.html', open_browser=False)
        print("\nChart saved → trendrider_backtest.html")

if __name__ == '__main__':
    main()
