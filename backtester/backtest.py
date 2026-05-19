#!/usr/bin/env python3
"""
backtest.py — TRENDRIDER v7 sequential backtester
Replays 1-min bars from Binance.US. Same logic as live_trader.py.
All indicators are causal (EMA/RSI/ATR/ADX) — no lookahead.

Usage:
  python backtest.py                        # last 30 days from Binance.US
  python backtest.py --days 90             # last 90 days
  python backtest.py --start 2026-01-01   # from date to now
  python backtest.py --csv data.csv       # from local CSV
  python backtest.py --save-csv data.csv  # fetch + save to CSV for reuse
"""

import os, sys, math, json, argparse
from datetime import datetime, timedelta, timezone
from pathlib import Path
import numpy as np

try:
    from binance.client import Client
    from dotenv import load_dotenv
    load_dotenv(Path(__file__).parent.parent / ".env")
    HAS_BINANCE = True
except ImportError:
    HAS_BINANCE = False

# ── Strategy parameters (keep in sync with live_trader.py) ───────────────────
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

EQUITY_PCT = 1.0
SYMBOL     = 'BTCUSDT'

# ── Indicators (identical to live_trader.py) ──────────────────────────────────
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
    avg_g  = np.mean(gains[:period])
    avg_l  = np.mean(losses[:period])
    for i in range(period, len(deltas)):
        avg_g = (avg_g * (period - 1) + gains[i]) / period
        avg_l = (avg_l * (period - 1) + losses[i]) / period
        rs = avg_g / avg_l if avg_l > 0 else float('inf')
        out[i + 1] = 100 - 100 / (1 + rs)
    return out

def calc_atr(bars, period=14):
    n   = len(bars)
    out = np.zeros(n)
    if n < 2:
        return out
    trs = np.array([
        max(bars[i]['high'] - bars[i]['low'],
            abs(bars[i]['high'] - bars[i-1]['close']),
            abs(bars[i]['low']  - bars[i-1]['close']))
        for i in range(1, n)
    ])
    if len(trs) < period:
        return out
    out[period] = np.mean(trs[:period])
    for i in range(period, len(trs)):
        out[i + 1] = (out[i] * (period - 1) + trs[i]) / period
    return out

def calc_adx(bars, period=14):
    n   = len(bars)
    out = np.zeros(n)
    if n < period * 2:
        return out
    plus_dm  = np.zeros(n)
    minus_dm = np.zeros(n)
    tr_arr   = np.zeros(n)
    for i in range(1, n):
        h, l   = bars[i]['high'], bars[i]['low']
        ph, pl = bars[i-1]['high'], bars[i-1]['low']
        pc     = bars[i-1]['close']
        up = h - ph; dn = pl - l
        plus_dm[i]  = up if up > dn and up > 0 else 0
        minus_dm[i] = dn if dn > up and dn > 0 else 0
        tr_arr[i]   = max(h - l, abs(h - pc), abs(l - pc))
    atr14 = np.zeros(n); pdm14 = np.zeros(n); mdm14 = np.zeros(n)
    atr14[period] = np.sum(tr_arr[1:period+1])
    pdm14[period] = np.sum(plus_dm[1:period+1])
    mdm14[period] = np.sum(minus_dm[1:period+1])
    for i in range(period + 1, n):
        atr14[i] = atr14[i-1] - atr14[i-1] / period + tr_arr[i]
        pdm14[i] = pdm14[i-1] - pdm14[i-1] / period + plus_dm[i]
        mdm14[i] = mdm14[i-1] - mdm14[i-1] / period + minus_dm[i]
    for i in range(period, n):
        if atr14[i] > 0:
            pdi = 100 * pdm14[i] / atr14[i]
            mdi = 100 * mdm14[i] / atr14[i]
            dx  = 100 * abs(pdi - mdi) / (pdi + mdi) if (pdi + mdi) > 0 else 0
            out[i] = dx if i == period else (out[i-1] * (period-1) + dx) / period
    return out

def calc_vol_ratio(volumes, period=10):
    n   = len(volumes)
    out = np.zeros(n)
    for i in range(period, n):
        avg    = float(np.mean(volumes[i-period:i]))
        out[i] = volumes[i] / avg if avg > 0 else 0.0
    return out

# ── Backtester ────────────────────────────────────────────────────────────────
def run_backtest(bars, initial_balance=10.0, verbose=False):
    n = len(bars)
    if n < 60:
        print("Not enough bars (need ≥60).")
        return [], [initial_balance]

    closes  = np.array([b['close']  for b in bars])
    volumes = np.array([b['volume'] for b in bars])

    # Pre-compute all indicators (causal — no lookahead)
    ema5  = calc_ema(closes, P['emaFast'])
    ema13 = calc_ema(closes, P['emaSlow'])
    ema50 = calc_ema(closes, P['emaTrend'])
    rsi_v = calc_rsi(closes)
    atr_v = calc_atr(bars)
    adx_v = calc_adx(bars)
    vol_v = calc_vol_ratio(volumes)

    usdt   = initial_balance
    pos    = None
    trades = []
    equity = [usdt]

    for i in range(55, n):
        bar   = bars[i]
        price = bar['close']
        f, f1 = ema5[i], ema5[i-1]
        s, s1 = ema13[i], ema13[i-1]
        tr = ema50[i]; r = rsi_v[i]; a = atr_v[i]; dx = adx_v[i]; v = vol_v[i]

        # ── Position management ───────────────────────────────────────────────
        if pos:
            pos['barsHeld'] += 1

            riskUnit = abs(pos['entry'] - pos['stop'])
            pnlR = (price - pos['entry']) / riskUnit if riskUnit > 0 else 0
            if pnlR >= P['trailActivate']:
                trail = price - pos['atr'] * P['trailAtr']
                pos['stop'] = max(pos['stop'], trail)

            if not pos['partialTaken'] and price >= pos['partialAt']:
                half = pos['qty'] * 0.5
                usdt += half * price
                pos['qty'] -= half
                pos['partialTaken'] = True
                trades.append({
                    'bar': i, 'ts': _ts(bar),
                    'setup': pos['setup'], 'outcome': 'PARTIAL',
                    'entry': pos['entry'], 'exit': price,
                    'qty': half, 'pnl': (price - pos['entry']) * half,
                })

            outcome = None; exit_px = price
            if price <= pos['stop']:
                outcome = 'STOPPED'; exit_px = pos['stop']
            elif price >= pos['target']:
                outcome = 'TARGET';  exit_px = pos['target']
            elif pos['barsHeld'] >= P['maxHoldBars']:
                outcome = 'TIMEOUT'

            if outcome:
                pnl = (exit_px - pos['entry']) * pos['qty']
                usdt += pos['qty'] * exit_px
                trades.append({
                    'bar': i, 'ts': _ts(bar),
                    'setup': pos['setup'], 'outcome': outcome,
                    'entry': pos['entry'], 'exit': exit_px,
                    'qty': pos['qty'], 'pnl': pnl,
                })
                if verbose:
                    sign = '+' if pnl >= 0 else ''
                    print(f"  [{i:6d}] EXIT  {pos['setup']:12} {outcome:8} "
                          f"${pos['entry']:.2f}→${exit_px:.2f}  {sign}${pnl:.4f}")
                pos = None

        # ── Signal detection ──────────────────────────────────────────────────
        if pos is None and dx >= P['adxMin'] and a > 0:
            setup = None

            if (f > s and f1 <= s1 and bar['close'] > tr and
                    v >= P['volMin'] and r < P['rsiOB']):
                setup = 'EMA_CROSS'

            elif (f > s and f1 > s1 and bar['close'] > tr and
                    bar['low'] <= f * 1.002 and bar['close'] > f and
                    v >= P['volMin'] and P['rsiOS'] < r < 55):
                setup = 'EMA_PULLBACK'

            if setup:
                entry = bar['close']
                qty   = (usdt * EQUITY_PCT) / entry
                if qty > 0 and qty * entry >= 1.0:
                    usdt -= qty * entry
                    pos = {
                        'setup': setup, 'entry': entry, 'qty': qty, 'atr': a,
                        'stop':      entry - a * P['atrStop'],
                        'target':    entry + a * P['atrTp'],
                        'partialAt': entry + a * P['partialAt'],
                        'barsHeld': 0, 'partialTaken': False,
                    }
                    if verbose:
                        print(f"  [{i:6d}] ENTRY {setup:12} @ ${entry:.2f}  "
                              f"stop=${pos['stop']:.2f}  target=${pos['target']:.2f}  "
                              f"adx={dx:.1f} rsi={r:.1f}")

        equity.append(usdt + (pos['qty'] * price if pos else 0))

    # Force-close open position at end of data
    if pos:
        price = bars[-1]['close']
        pnl   = (price - pos['entry']) * pos['qty']
        usdt  += pos['qty'] * price
        trades.append({
            'bar': n-1, 'ts': _ts(bars[-1]),
            'setup': pos['setup'], 'outcome': 'EOD',
            'entry': pos['entry'], 'exit': price,
            'qty': pos['qty'], 'pnl': pnl,
        })

    return trades, equity

def _ts(bar):
    t = bar.get('time', 0)
    return datetime.fromtimestamp(t/1000, tz=timezone.utc).strftime('%Y-%m-%d %H:%M') if t else ''

# ── Reporting ─────────────────────────────────────────────────────────────────
def compute_sharpe(pnls):
    if len(pnls) < 2:
        return float('nan')
    arr = np.array(pnls)
    std = np.std(arr, ddof=1)
    return float(np.mean(arr) / std) if std > 0 else float('nan')

def report(trades, equity, initial_balance, out_json=None):
    full   = [t for t in trades if t['outcome'] != 'PARTIAL']
    wins   = [t for t in full   if t['pnl'] > 0]
    total_pnl = sum(t['pnl'] for t in trades)
    sharpe    = compute_sharpe([t['pnl'] for t in full])

    eq   = np.array(equity)
    peak = np.maximum.accumulate(eq)
    mdd  = float(np.max((peak - eq) / np.where(peak > 0, peak, 1))) * 100

    by_setup = {}
    by_outcome = {}
    for t in full:
        s = t['setup']
        by_setup.setdefault(s, {'n': 0, 'w': 0, 'pnl': 0.0})
        by_setup[s]['n'] += 1
        if t['pnl'] > 0: by_setup[s]['w'] += 1
        by_setup[s]['pnl'] += t['pnl']
        o = t['outcome']
        by_outcome.setdefault(o, {'n': 0, 'pnl': 0.0})
        by_outcome[o]['n'] += 1
        by_outcome[o]['pnl'] += t['pnl']

    print()
    print("=" * 62)
    print("  TRENDRIDER v7  —  BACKTEST RESULTS")
    print("=" * 62)
    print(f"  Starting balance:  ${initial_balance:.2f}")
    print(f"  Ending balance:    ${equity[-1]:.4f}")
    print(f"  Net P&L:           ${total_pnl:+.4f}  ({(equity[-1]-initial_balance)/initial_balance*100:+.2f}%)")
    print(f"  Max drawdown:      {mdd:.2f}%")
    print(f"  Sharpe (trades):   {sharpe:.3f}")
    print(f"  Total bars:        {len(equity)-1:,}")
    print(f"  Total exits:       {len(full)}")
    print(f"  Win rate:          {len(wins)/len(full)*100:.1f}%  ({len(wins)}/{len(full)})")
    print()
    print("  By setup:")
    for s, v in by_setup.items():
        wr = v['w']/v['n']*100 if v['n'] else 0
        print(f"    {s:14}  {v['n']:3d} trades  {wr:5.1f}% wins  ${v['pnl']:+.4f}")
    print()
    print("  By outcome:")
    for o, v in sorted(by_outcome.items()):
        print(f"    {o:10}  {v['n']:3d}  ${v['pnl']:+.4f}")
    print()
    print("  Last 15 trades:")
    for t in trades[-15:]:
        sign = '+' if t['pnl'] >= 0 else ''
        print(f"    {t['ts']}  {t['setup']:12} {t['outcome']:8} "
              f"${t['entry']:.2f}→${t['exit']:.2f}  {sign}${t['pnl']:.5f}")
    print("=" * 62)

    if out_json:
        result = {
            'sharpe': sharpe, 'win_rate': len(wins)/len(full) if full else 0,
            'total_pnl': total_pnl, 'return_pct': (equity[-1]-initial_balance)/initial_balance*100,
            'max_drawdown_pct': mdd, 'n_trades': len(full), 'trades': trades,
            'P': P,
        }
        Path(out_json).write_text(json.dumps(result, indent=2))
        print(f"\n  Results saved to {out_json}")

# ── Data fetching ─────────────────────────────────────────────────────────────
def fetch_binance(days=30, start=None):
    if not HAS_BINANCE:
        sys.exit("python-binance not installed: pip install python-binance")
    api_key    = os.getenv("BINANCE_API_KEY", "")
    api_secret = os.getenv("BINANCE_API_SECRET", "")
    client = Client(api_key, api_secret, tld='us')

    now_ms = int(datetime.now(timezone.utc).timestamp() * 1000)
    if start:
        start_dt = datetime.strptime(start, '%Y-%m-%d').replace(tzinfo=timezone.utc)
        start_ms = int(start_dt.timestamp() * 1000)
    else:
        start_ms = now_ms - days * 86400 * 1000

    print(f"Fetching {SYMBOL} 1m klines from Binance.US...")
    raw = []
    cur = start_ms
    while cur < now_ms:
        chunk = client.get_klines(symbol=SYMBOL, interval='1m', startTime=cur, limit=1000)
        if not chunk:
            break
        raw.extend(chunk)
        cur = chunk[-1][0] + 60000
        print(f"  {len(raw):,} bars...", end='\r', flush=True)
    print(f"\nFetched {len(raw):,} bars.")

    return [{'time': c[0], 'open': float(c[1]), 'high': float(c[2]),
             'low': float(c[3]), 'close': float(c[4]), 'volume': float(c[5])}
            for c in raw]

def load_csv(path):
    import csv
    bars = []
    with open(path) as f:
        reader = csv.DictReader(f)
        for row in reader:
            bars.append({'time': int(float(row.get('time', 0))),
                         'open':   float(row['open']),
                         'high':   float(row['high']),
                         'low':    float(row['low']),
                         'close':  float(row['close']),
                         'volume': float(row['volume'])})
    return bars

def save_csv(bars, path):
    import csv
    with open(path, 'w', newline='') as f:
        w = csv.DictWriter(f, fieldnames=['time','open','high','low','close','volume'])
        w.writeheader()
        w.writerows(bars)
    print(f"Saved {len(bars):,} bars → {path}")

# ── Entry point ───────────────────────────────────────────────────────────────
def main():
    ap = argparse.ArgumentParser(description='TRENDRIDER v7 backtester')
    ap.add_argument('--days',     type=int,   default=30,    help='Days of history (default 30)')
    ap.add_argument('--start',    type=str,   default=None,  help='Start date YYYY-MM-DD')
    ap.add_argument('--csv',      type=str,   default=None,  help='Load bars from CSV')
    ap.add_argument('--save-csv', type=str,   default=None,  help='Save fetched bars to CSV')
    ap.add_argument('--balance',  type=float, default=100.0, help='Starting balance USD (default 100)')
    ap.add_argument('--verbose',  action='store_true',       help='Print every entry/exit')
    ap.add_argument('--out-json', type=str,   default=None,  help='Save results JSON')
    args = ap.parse_args()

    if args.csv:
        bars = load_csv(args.csv)
        print(f"Loaded {len(bars):,} bars from {args.csv}")
    else:
        bars = fetch_binance(days=args.days, start=args.start)
        if args.save_csv:
            save_csv(bars, args.save_csv)

    print(f"Running backtest: {len(bars):,} bars  ${args.balance:.2f} start  "
          f"adxMin={P['adxMin']} rsiOB={P['rsiOB']} atrTp={P['atrTp']}")

    trades, equity = run_backtest(bars, initial_balance=args.balance, verbose=args.verbose)
    report(trades, equity, args.balance, out_json=args.out_json)

if __name__ == '__main__':
    main()
