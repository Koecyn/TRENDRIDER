#!/usr/bin/env python3
"""
DOWNTREND/strategy_downtrend.py — TRENDRIDER v7, regime-adaptive variant.

Key difference from strategy_bt.py:
  Uses DIRECTIONAL ATR — stop is sized by downside volatility, target by
  upside volatility. In a downtrend the market's upward ATR shrinks, so
  targets tighten automatically. In an uptrend, upward ATR expands and
  targets widen. A minBias gate skips entries when the market is too
  one-sided to the downside to be tradeable long.

  bias = atr_up / (atr_up + atr_dn)
    → 1.0 : all upward movement (pure uptrend)
    → 0.5 : balanced (ranging / flat)
    → 0.0 : all downward movement (pure downtrend)

  stop   = entry - atr_dn * atrStop   (sized to actual downward swings)
  target = entry + atr_up * atrTp     (sized to actual upward swings)

Usage:
  python DOWNTREND/strategy_downtrend.py --csv btc_30d.csv
  python DOWNTREND/strategy_downtrend.py --csv btc_90d.csv --balance 100
  python DOWNTREND/strategy_downtrend.py --csv btc_90d.csv --optimize
  python DOWNTREND/strategy_downtrend.py --days 30          # live fetch
"""

import sys, argparse
from pathlib import Path
import numpy as np
import pandas as pd
from backtesting import Backtest, Strategy

# Allow importing sibling modules (data fetching shared with strategy_bt.py)
sys.path.insert(0, str(Path(__file__).parent.parent))
try:
    from strategy_bt import fetch_binance, load_csv, save_csv
except ImportError:
    fetch_binance = load_csv = save_csv = None

# ── Parameters ────────────────────────────────────────────────────────────────
P = {
    'emaFast':       5,
    'emaSlow':       13,
    'emaTrend':      50,
    'rsiOB':         80,
    'rsiOS':         28,
    'volMin':        0.0,
    'atrStop':       1.9,    # multiplier applied to atr_dn for stop
    'atrTp':         2.8,    # multiplier applied to atr_up for target
    'partialAt':     1.0,    # partial exit at 1× atr_up
    'trailAtr':      0.65,
    'trailActivate': 0.25,
    'maxHoldBars':   25,
    'adxMin':        22,
    'minBias':       -0.1,   # skip entry if bias < -0.1 (market too bearish to go long)
}

# ── Indicators ────────────────────────────────────────────────────────────────
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
    n = len(close)
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

def calc_atr_bias(high, low, close, period=14):
    """
    Directional ATR bias normalised to [-1, +1].

    bias = (atr_up - atr_dn) / (atr_up + atr_dn)

    +1  price fails to reclaim lows  → buyers absorb every dip  → pure uptrend
     0  symmetric up/down movement   → flat / ranging
    -1  price fails to reclaim highs → sellers cap every bounce → pure downtrend

    atr_up : Wilder-smoothed mean of max(high - prev_close, 0)
    atr_dn : Wilder-smoothed mean of max(prev_close - low,  0)

    Use with total ATR to get expected directional components:
      expected_up = atr * (1 + bias) / 2
      expected_dn = atr * (1 - bias) / 2
    """
    n = len(close)
    out = np.zeros(n)
    if n <= period + 1:
        return out
    up = np.array([max(high[i] - close[i-1], 0.0) for i in range(1, n)])
    dn = np.array([max(close[i-1] - low[i],  0.0) for i in range(1, n)])

    au = np.mean(up[:period])
    ad = np.mean(dn[:period])
    tot = au + ad
    out[period] = (au - ad) / tot if tot > 0 else 0.0

    for i in range(period, len(up)):
        au = (au * (period - 1) + up[i]) / period
        ad = (ad * (period - 1) + dn[i]) / period
        tot = au + ad
        out[i + 1] = (au - ad) / tot if tot > 0 else 0.0
    return out

def calc_adx(high, low, close, period=14):
    n = len(close)
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
    n = len(volumes)
    out = np.zeros(n)
    for i in range(period, n):
        avg = np.mean(volumes[i-period:i])
        out[i] = volumes[i] / avg if avg > 0 else 0.0
    return out

# ── Strategy ──────────────────────────────────────────────────────────────────
class TrendRiderAdaptive(Strategy):
    # Tunable params
    adxMin        = P['adxMin']
    rsiOB         = P['rsiOB']
    atrStop       = P['atrStop']
    atrTp         = P['atrTp']
    partialAt     = P['partialAt']
    trailAtr      = P['trailAtr']
    trailActivate = P['trailActivate']
    maxHoldBars   = P['maxHoldBars']
    minBias       = P['minBias']   # min upside fraction to allow entry

    def init(self):
        c = np.array(self.data.Close)
        h = np.array(self.data.High)
        l = np.array(self.data.Low)
        v = np.array(self.data.Volume)

        self.ema5     = self.I(calc_ema,      c,       P['emaFast'],  name='EMA5')
        self.ema13    = self.I(calc_ema,      c,       P['emaSlow'],  name='EMA13')
        self.ema50    = self.I(calc_ema,      c,       P['emaTrend'], name='EMA50')
        self.rsi      = self.I(calc_rsi,      c,                      name='RSI')
        self.atr      = self.I(calc_atr,      h, l, c,                name='ATR')
        self.atr_bias = self.I(calc_atr_bias, h, l, c,                name='ATR_Bias')
        self.adx      = self.I(calc_adx,      h, l, c,                name='ADX')
        self.vol      = self.I(calc_vol_ratio,v,                       name='VolRatio')

        self._stop          = 0.0
        self._target        = 0.0
        self._partial_at    = 0.0
        self._partial_taken = False
        self._bars_held     = 0
        self._entry         = 0.0
        self._atr_up_entry  = 0.0

        # Diagnostics
        self._d_bars        = 0
        self._d_in_pos      = 0
        self._d_no_atr      = 0
        self._d_low_adx     = 0
        self._d_low_bias    = 0
        self._d_ema_bear    = 0
        self._d_below_tr    = 0
        self._d_rsi_block   = 0
        self._d_cross       = 0
        self._d_pullback    = 0
        self._d_bias_sum    = 0.0
        self._d_bias_n      = 0

    def next(self):
        price = self.data.Close[-1]
        f,  f1 = self.ema5[-1],  self.ema5[-2]
        s,  s1 = self.ema13[-1], self.ema13[-2]
        tr      = self.ema50[-1]
        r       = self.rsi[-1]
        dx      = self.adx[-1]
        lo      = self.data.Low[-1]

        a    = self.atr[-1]
        bias = self.atr_bias[-1]   # -1 (downtrend) → 0 (flat) → +1 (uptrend)

        # Expected directional components from total ATR + bias
        # atr * (1+bias)/2 = upward expected move   (0 when bias=-1, atr when bias=+1)
        # atr * (1-bias)/2 = downward expected move  (atr when bias=-1, 0 when bias=+1)
        FLOOR = 0.15   # never let either component drop below 15% of total ATR
        a_up_exp = max(a * (1 + bias) / 2, a * FLOOR)
        a_dn_exp = max(a * (1 - bias) / 2, a * FLOOR)

        self._d_bars += 1

        # ── Manage open position ──────────────────────────────────────────
        if self.position:
            self._bars_held += 1
            self._d_in_pos  += 1

            riskUnit = abs(self._entry - self._stop)
            pnlR = (price - self._entry) / riskUnit if riskUnit > 0 else 0
            if pnlR >= self.trailActivate:
                trail = price - self._a_up_entry * self.trailAtr
                self._stop = max(self._stop, trail)

            if not self._partial_taken and price >= self._partial_at:
                self.position.close(0.5)
                self._partial_taken = True

            if price <= self._stop or price >= self._target or self._bars_held >= self.maxHoldBars:
                self.position.close()
            return

        # ── Gate: indicators must be warmed up ───────────────────────────
        if a <= 0:
            self._d_no_atr += 1
            return
        if dx < self.adxMin:
            self._d_low_adx += 1
            return

        # ── Regime gate — bias on [-1, +1] scale ─────────────────────────
        self._d_bias_sum += bias
        self._d_bias_n   += 1

        if bias < self.minBias:   # too bearish to go long
            self._d_low_bias += 1
            return

        # ── Signal detection ──────────────────────────────────────────────
        setup = None
        if f <= s:
            self._d_ema_bear += 1
        elif price <= tr:
            self._d_below_tr += 1
        else:
            if f1 <= s1:
                if r < self.rsiOB:
                    setup = 'EMA_CROSS'
                else:
                    self._d_rsi_block += 1
            else:
                if lo <= f * 1.002 and price > f and P['rsiOS'] < r < 55:
                    setup = 'EMA_PULLBACK'
                else:
                    self._d_rsi_block += 1

        if setup == 'EMA_CROSS':
            self._d_cross += 1
        elif setup == 'EMA_PULLBACK':
            self._d_pullback += 1

        if setup:
            self._entry         = price
            # Stop/target sized to expected directional components
            self._stop          = price - a_dn_exp * self.atrStop
            self._target        = price + a_up_exp * self.atrTp
            self._partial_at    = price + a_up_exp * self.partialAt
            self._partial_taken = False
            self._bars_held     = 0
            self._a_up_entry    = a_up_exp
            self.buy(size=0.99)

# ── Entry point ───────────────────────────────────────────────────────────────
def main():
    ap = argparse.ArgumentParser(
        description='TRENDRIDER DOWNTREND — regime-adaptive directional ATR')
    ap.add_argument('--days',     type=int,   default=7)
    ap.add_argument('--start',    type=str,   default=None)
    ap.add_argument('--csv',      type=str,   default=None)
    ap.add_argument('--save-csv', type=str,   default=None)
    ap.add_argument('--balance',  type=float, default=100.0)
    ap.add_argument('--optimize', action='store_true')
    args = ap.parse_args()

    if args.csv:
        if load_csv is None:
            sys.exit("strategy_bt.py not found — run from backtester/ parent directory")
        df = load_csv(args.csv)
        print(f"Loaded {len(df):,} bars from {args.csv}")
    else:
        if fetch_binance is None:
            sys.exit("python-binance not installed: pip install python-binance")
        df = fetch_binance(days=args.days, start=args.start)
        if args.save_csv:
            save_csv(df, args.save_csv)

    # Scale prices so backtesting.py produces integer unit counts > 0
    med   = df['Close'].median()
    scale = max(1, round(med / (args.balance / 100)))
    if scale > 1:
        df[['Open', 'High', 'Low', 'Close']] /= scale
        print(f"Price scaled by 1/{scale} (median ${med:,.0f} → ${med/scale:.2f}) — "
              f"~{round(0.99 * args.balance / (med / scale))} units per trade")

    bt = Backtest(df, TrendRiderAdaptive, cash=args.balance,
                  commission=0.0, margin=1.0, exclusive_orders=True)

    if args.optimize:
        print("Optimizing...")
        stats = bt.optimize(
            adxMin    = range(12, 25, 3),
            rsiOB     = range(72, 88, 4),
            atrStop   = [1.2, 1.5, 1.9, 2.3],
            atrTp     = [2.0, 2.5, 2.8, 3.5],
            minBias   = [0.25, 0.35, 0.45],
            trailAtr  = [0.5, 0.65, 0.8],
            maximize  = 'Sharpe Ratio',
            constraint= lambda p: p.atrTp > p.atrStop,
            return_heatmap=False,
        )
        print("\n── Optimal parameters ──")
        print(stats._strategy)
    else:
        stats = bt.run()
        print(stats)

        st = stats._strategy
        total = st._d_bars or 1
        avg_bias = st._d_bias_sum / st._d_bias_n if st._d_bias_n else 0
        regime = ('uptrend'   if avg_bias >  0.1 else
                  'downtrend' if avg_bias < -0.1 else 'ranging/flat')
        print(f"\n── Regime diagnostic ({st._d_bars} bars) ──")
        print(f"  Avg ATR bias (-1=down, 0=flat, +1=up): {avg_bias:+.3f}  ({regime})")
        print(f"  In position:          {st._d_in_pos:6d}  ({100*st._d_in_pos/total:.1f}%)")
        print(f"  ATR not ready:        {st._d_no_atr:6d}  ({100*st._d_no_atr/total:.1f}%)")
        print(f"  ADX < {P['adxMin']} blocked:  {st._d_low_adx:6d}  ({100*st._d_low_adx/total:.1f}%)")
        print(f"  Bias < {P['minBias']} (too bearish): {st._d_low_bias:6d}  ({100*st._d_low_bias/total:.1f}%)")
        print(f"  EMA bear (f≤s):       {st._d_ema_bear:6d}  ({100*st._d_ema_bear/total:.1f}%)")
        print(f"  Price < EMA50:        {st._d_below_tr:6d}  ({100*st._d_below_tr/total:.1f}%)")
        print(f"  RSI/pullback blocked: {st._d_rsi_block:6d}  ({100*st._d_rsi_block/total:.1f}%)")
        print(f"  EMA_CROSS fired:      {st._d_cross:6d}")
        print(f"  EMA_PULLBACK fired:   {st._d_pullback:6d}")
        print(f"  Total trades:         {int(stats['# Trades']):6d}")

        out_html = Path(__file__).parent / 'trendrider_downtrend.html'
        bt.plot(filename=str(out_html), open_browser=False)
        print(f"\nChart saved → {out_html}")


if __name__ == '__main__':
    main()
