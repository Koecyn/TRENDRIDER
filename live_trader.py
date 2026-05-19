#!/usr/bin/env python3
"""
live_trader.py — TRENDRIDER Micro-Trend Scalper v7
Binance.US | WebSocket 1-min klines | MAKER-only entries | Long-only
EMA_CROSS + EMA_PULLBACK | Partial profit + trailing stop
"""
import os, sys, time, math, threading
from datetime import datetime
from collections import deque
import numpy as np
from dotenv import load_dotenv
from binance.client import Client
from binance import ThreadedWebsocketManager
from binance.exceptions import BinanceAPIException

load_dotenv()

IS_TTY = sys.stdout.isatty()   # False when run as subprocess → compact output

# ── Strategy parameters (ported 1:1 from strategy.js PARAMS) ─────────────────
P = {
    'emaFast':       5,
    'emaSlow':       13,
    'emaTrend':      50,
    'rsiOB':         75,
    'rsiOS':         28,
    'volMin':        0.50,
    'atrStop':       1.9,
    'atrTp':         2.8,
    'partialAt':     1.0,
    'trailAtr':      0.65,
    'trailActivate': 0.25,
    'maxHoldBars':   25,
    'riskPct':       0.025,
    'adxMin':        18,
}

SYMBOL        = "BTCUSDT"
EQUITY_PCT    = 1.0       # use full balance — maximises notional on small account
TAKER_FEE     = 0.00020
SELL_WINDOW   = 30
ORDER_TIMEOUT = 45
W             = 70

G='\033[92m'; R='\033[91m'; Y='\033[93m'; C='\033[96m'; B='\033[1m'; Z='\033[0m'

# ── Indicators (ported from strategy.js) ──────────────────────────────────────
def calc_ema(arr, period):
    k   = 2.0 / (period + 1)
    out = np.full(len(arr), np.nan)
    if len(arr) < period:
        return out
    out[period - 1] = float(np.mean(arr[:period]))
    for i in range(period, len(arr)):
        out[i] = arr[i] * k + out[i - 1] * (1 - k)
    return out

def calc_rsi(closes, period=14):
    out = np.full(len(closes), np.nan)
    for i in range(period, len(closes)):
        g = l = 0.0
        for j in range(i - period + 1, i + 1):
            d = closes[j] - closes[j - 1]
            if d > 0: g += d
            else:     l -= d
        ag = g / period; al = l / period
        out[i] = 100.0 if al == 0 else 100.0 - 100.0 / (1 + ag / al)
    return out

def calc_atr(clist, period=14):
    n   = len(clist)
    out = np.full(n, np.nan)
    s   = 0.0
    for i in range(1, n):
        tr = max(
            clist[i]['high'] - clist[i]['low'],
            abs(clist[i]['high'] - clist[i-1]['close']),
            abs(clist[i]['low']  - clist[i-1]['close']),
        )
        if i <= period:
            s += tr
            if i == period: out[i] = s / period
        else:
            out[i] = (out[i-1] * (period - 1) + tr) / period
    return out

def calc_vol_ratio(volumes, period=10):
    n   = len(volumes)
    out = np.zeros(n)
    for i in range(period, n):
        avg    = float(np.mean(volumes[i - period:i]))
        out[i] = volumes[i] / avg if avg > 0 else 0.0
    return out

def calc_adx(clist, period=14):
    n   = len(clist)
    out = np.full(n, np.nan)
    trs, pdms, mdms = [], [], []
    for i in range(1, n):
        h  = clist[i]['high'];    l  = clist[i]['low']
        ph = clist[i-1]['high'];  pl = clist[i-1]['low']
        pc = clist[i-1]['close']
        trs.append(max(h - l, abs(h - pc), abs(l - pc)))
        up = h - ph; dn = pl - l
        pdms.append(up if (up > dn and up > 0) else 0.0)
        mdms.append(dn if (dn > up and dn > 0) else 0.0)
    if len(trs) < period:
        return out
    s_tr = sum(trs[:period])
    s_p  = sum(pdms[:period])
    s_m  = sum(mdms[:period])
    dx_list = []
    for i in range(period, len(trs)):
        s_tr = s_tr - s_tr / period + trs[i]
        s_p  = s_p  - s_p  / period + pdms[i]
        s_m  = s_m  - s_m  / period + mdms[i]
        pd   = (s_p / s_tr * 100) if s_tr > 0 else 0.0
        md   = (s_m / s_tr * 100) if s_tr > 0 else 0.0
        dx_list.append(abs(pd - md) / (pd + md) * 100 if (pd + md) > 0 else 0.0)
    if len(dx_list) < period:
        return out
    adx_v = sum(dx_list[:period]) / period
    if period * 2 < n:
        out[period * 2] = adx_v
    for i in range(period, len(dx_list)):
        adx_v = (adx_v * (period - 1) + dx_list[i]) / period
        idx   = i + period + 1
        if idx < n:
            out[idx] = adx_v
    return out

# ── Signal engine — long-only ─────────────────────────────────────────────────
def get_signal(clist):
    if len(clist) < 55:
        return None
    closes  = np.array([c['close']  for c in clist])
    volumes = np.array([c['volume'] for c in clist])
    i       = len(clist) - 1

    fast_a  = calc_ema(closes, P['emaFast'])
    slow_a  = calc_ema(closes, P['emaSlow'])
    trend_a = calc_ema(closes, P['emaTrend'])
    rsi_a   = calc_rsi(closes)
    atr_a   = calc_atr(clist)
    vol_a   = calc_vol_ratio(volumes)
    adx_a   = calc_adx(clist)

    f  = fast_a[i];  f1 = fast_a[i - 1]
    s  = slow_a[i];  s1 = slow_a[i - 1]
    tr = trend_a[i]
    r  = rsi_a[i]
    a  = atr_a[i]
    v  = vol_a[i]
    dx = adx_a[i]
    cn = clist[i]

    if any(np.isnan(x) for x in [f, f1, s, s1, tr, r, a, dx]):
        return None, 'indicators not ready'
    if dx < P['adxMin']:
        return None, f'ADX {dx:.1f} < {P["adxMin"]} (choppy)'

    base = {'atr': a, 'rsi': r, 'adx': dx, 'vol': v,
            'time': cn['time'], 'barsHeld': 0, 'partialTaken': False}

    if (f > s and f1 <= s1 and cn['close'] > tr and
            v >= P['volMin'] and r < P['rsiOB']):
        return {**base, 'setup': 'EMA_CROSS', 'direction': 'LONG',
                'entry':    cn['close'],
                'stop':     cn['close'] - a * P['atrStop'],
                'target':   cn['close'] + a * P['atrTp'],
                'partialAt': cn['close'] + a * P['partialAt']}, None

    if (f > s and f1 > s1 and cn['close'] > tr and
            cn['low'] <= f * 1.002 and cn['close'] > f and
            v >= P['volMin'] and P['rsiOS'] < r < 55):
        return {**base, 'setup': 'EMA_PULLBACK', 'direction': 'LONG',
                'entry':    cn['close'],
                'stop':     cn['close'] - a * P['atrStop'],
                'target':   cn['close'] + a * P['atrTp'],
                'partialAt': cn['close'] + a * P['partialAt']}, None

    # Explain why no setup fired
    reasons = []
    cross_now  = f > s;  cross_prev = f1 <= s1
    above_tr   = cn['close'] > tr
    vol_ok     = v >= P['volMin']
    if not above_tr:
        reasons.append(f'price ${cn["close"]:,.0f} < EMA50 ${tr:,.0f}')
    if not vol_ok:
        reasons.append(f'VolRatio {v:.3f}× < {P["volMin"]}')
    if cross_now and cross_prev:             # EMA_PULLBACK territory
        if r >= 55:
            reasons.append(f'RSI {r:.1f} ≥ 55 (pullback needs <55)')
        elif r < P['rsiOS']:
            reasons.append(f'RSI {r:.1f} < {P["rsiOS"]} (oversold)')
        if not (cn['low'] <= f * 1.002):
            reasons.append('no EMA touch (pullback needs low≤EMA5)')
    if cross_now and not cross_prev and r >= P['rsiOB']:
        reasons.append(f'RSI {r:.1f} ≥ {P["rsiOB"]} (overbought)')
    if not cross_now:
        reasons.append(f'EMA bearish (F{f:,.0f}<S{s:,.0f})')
    return None, ' | '.join(reasons) if reasons else 'no setup'

# ── Exchange helpers ──────────────────────────────────────────────────────────
def get_filters(client):
    f = {'min_notional': 1.0, 'step': 0.00001, 'tick': 0.01, 'min_qty': 0.00001}
    for flt in client.get_symbol_info(SYMBOL)['filters']:
        ft = flt['filterType']
        if ft == 'LOT_SIZE':
            f['step']    = float(flt['stepSize'])
            f['min_qty'] = float(flt['minQty'])
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
    return f"{floor_qty(qty, step):.{p}f}"

def fmt_px(price, tick):
    p = max(0, round(-math.log10(tick)))
    return f"{round(price, p):.{p}f}"

def get_balances(client):
    bal = {b['asset']: float(b['free']) for b in client.get_account()['balances']}
    return bal.get('BTC', 0.0), bal.get('USDT', 0.0)

def best_bid_ask(client):
    bk = client.get_order_book(symbol=SYMBOL, limit=5)
    return float(bk['bids'][0][0]), float(bk['asks'][0][0])

def cancel_open_orders(client):
    try:
        orders = client.get_open_orders(symbol=SYMBOL)
        for o in orders:
            client.cancel_order(symbol=SYMBOL, orderId=o['orderId'])
        if orders:
            print(f"{Y}Cancelled {len(orders)} open order(s){Z}", flush=True)
    except BinanceAPIException as e:
        print(f"{R}Cancel error: {e.message}{Z}", flush=True)

# ── Maker sell — market only if profitable after taker fee ────────────────────
def sell_best(client, flt, qty, entry_price, label=""):
    step = flt['step']; tick = flt['tick']
    qty_s    = fmt_qty(qty, step)
    deadline = time.time() + SELL_WINDOW
    order_id = None

    while True:
        _, ask = best_bid_ask(client)
        limit_px = float(fmt_px(ask + tick, tick))

        if order_id:
            try: client.cancel_order(symbol=SYMBOL, orderId=order_id)
            except BinanceAPIException: pass
            order_id = None

        try:
            o = client.create_order(
                symbol=SYMBOL, side='SELL', type='LIMIT_MAKER',
                quantity=qty_s, price=fmt_px(limit_px, tick))
            order_id = o['orderId']
        except BinanceAPIException as e:
            print(f"{R}[{label}] sell err: {e.message}{Z}", flush=True)
            time.sleep(1)
            continue

        while time.time() < deadline:
            time.sleep(0.25)
            try:
                o = client.get_order(symbol=SYMBOL, orderId=order_id)
            except BinanceAPIException:
                continue
            if o['status'] == 'FILLED':
                fill_px = float(o['cummulativeQuoteQty']) / float(o['executedQty'])
                return fill_px, float(o['executedQty']), 'MAKER'

        _, ask = best_bid_ask(client)
        fee_cost = ask * qty * TAKER_FEE
        profit   = (ask - entry_price) * qty
        if profit > fee_cost:
            try: client.cancel_order(symbol=SYMBOL, orderId=order_id)
            except BinanceAPIException: pass
            try:
                o = client.order_market_sell(symbol=SYMBOL, quantity=qty_s)
                fills = o.get('fills', [])
                if fills:
                    tq = sum(float(f['qty'])   for f in fills)
                    wp = sum(float(f['price']) * float(f['qty']) for f in fills) / tq
                    return wp, tq, 'MARKET'
            except BinanceAPIException as e:
                print(f"{R}Market sell err: {e.message}{Z}", flush=True)

        deadline = time.time() + SELL_WINDOW
        print(f"{Y}[{label}] not profitable for market — re-placing maker{Z}", flush=True)

# ── Dashboard — full screen (TTY) ─────────────────────────────────────────────
def _draw_tty(state):
    sys.stdout.write('\033[2J\033[H')
    price   = state.get('price', 0)
    btc     = state.get('btc', 0.0)
    usdt    = state.get('usdt', 0.0)
    pos     = state.get('pos')
    trades  = state.get('trades', [])
    ind     = state.get('ind', {})
    bars    = state.get('bars', 0)
    status  = state.get('status', 'INIT')
    pending = state.get('pending_order')
    total   = usdt + btc * price
    tqty    = state.get('trade_qty', 0.0)
    tpx     = state.get('trade_px', 0.0)
    tside   = state.get('trade_side', '---')

    print(f"{C}{'═'*W}{Z}")
    print(f"  {B}TRENDRIDER v7{Z}  BTCUSDT  {C}${price:,.2f}{Z}  "
          f"{datetime.now().strftime('%H:%M:%S')}  [{status}]")
    print(f"  Wallet  {G}${usdt:.8f} USDT{Z}  {btc:.8f} BTC  Total ${total:.8f}")
    print(f"{C}{'─'*W}{Z}")

    if ind:
        fc = G if ind.get('fast', 0) > ind.get('slow', 0) else R
        dir_arrow = '↑' if ind.get('fast', 0) > ind.get('slow', 0) else '↓'
        vc = G if ind.get('vol', 0) >= P['volMin'] else R
        print(f"  EMA {fc}{dir_arrow}{Z} F:{ind['fast']:,.2f}  S:{ind['slow']:,.2f}  "
              f"T:{ind['trend']:,.2f}   ATR ${ind['atr']:.8f}   ADX {ind['adx']:.8f}")
        rc = G if ind['rsi'] < 40 else (R if ind['rsi'] > 65 else Z)
        sc = G if tside == 'BUY' else R
        print(f"  RSI {rc}{ind['rsi']:.8f}{Z}   "
              f"VolRatio {vc}{ind['vol']:.3f}×{Z}  "
              f"LastBTC {ind.get('last_vol',0):.8f}  "
              f"10BarAvg {ind.get('vol_avg',0):.8f}")
        print(f"  Last trade: {sc}{tside}{Z}  {tqty:.8f} BTC @ ${tpx:,.2f}")
    else:
        print(f"  Warming up… {bars}/55 candles needed")

    print(f"{C}{'─'*W}{Z}")

    # Pending order
    if pending:
        elapsed = time.time() - pending.get('placed_at', time.time())
        remain  = max(0, ORDER_TIMEOUT - elapsed)
        print(f"  {Y}⏳ PENDING BUY  LIMIT_MAKER @ ${pending['px']:,.2f}  "
              f"qty {pending['qty']:.6f}  ({remain:.0f}s){Z}")
        print(f"{C}{'─'*W}{Z}")

    # Active position
    if pos:
        held = pos.get('barsHeld', 0)
        qty  = pos.get('qty', 0.0)
        unr  = (price - pos['entry']) * qty
        uc   = G if unr >= 0 else R
        pt   = f"{G}✓ partial taken{Z}" if pos.get('partialTaken') else f"{Y}○ partial pending{Z}"
        stop_dist = price - pos['stop']
        tgt_dist  = pos['target'] - price
        print(f"  {G}▶ POSITION{Z}  {B}{pos['setup']}{Z}  "
              f"entry ${pos['entry']:,.2f}  qty {qty:.6f}")
        print(f"    stop ${pos['stop']:,.2f} (${stop_dist:.2f} away)  "
              f"target ${pos['target']:,.2f} (${tgt_dist:.2f} away)  "
              f"bars {held}/{P['maxHoldBars']}")
        print(f"    unrealized {uc}{unr:+.5f}{Z}  {pt}")
    elif not pending:
        print(f"  {Y}{state.get('sig_msg', 'scanning…')}{Z}")

    print(f"{C}{'─'*W}{Z}")

    # Trade history
    wins = sum(1 for t in trades if t['pnl'] > 0)
    net  = sum(t['pnl'] for t in trades)
    nc   = G if net >= 0 else R
    print(f"  Trades {len(trades)}  Wins {wins}  Net {nc}{net:+.5f}{Z}")
    for t in reversed(trades[-3:]):
        tc = G if t['pnl'] > 0 else R
        print(f"  [{t['setup']}] {t['outcome']}  "
              f"${t['entry']:,.2f}→${t['exit']:,.2f}  "
              f"{tc}{t['pnl']:+.5f}{Z}  via {t.get('via','?')}")
    print(f"{C}{'═'*W}{Z}")
    sys.stdout.flush()

# ── Dashboard — compact scrolling (subprocess / tail -f) ─────────────────────
def _draw_compact(state):
    price   = state.get('price', 0)
    btc     = state.get('btc', 0.0)
    usdt    = state.get('usdt', 0.0)
    pos     = state.get('pos')
    trades  = state.get('trades', [])
    ind     = state.get('ind', {})
    bars    = state.get('bars', 0)
    status  = state.get('status', '')
    pending = state.get('pending_order')
    total   = usdt + btc * price
    now     = datetime.now().strftime('%H:%M:%S')
    wins    = sum(1 for t in trades if t['pnl'] > 0)
    net     = sum(t['pnl'] for t in trades)
    tqty    = state.get('trade_qty', 0.0)
    tpx     = state.get('trade_px', 0.0)
    tside   = state.get('trade_side', '---')

    sep = f"{C}{'─'*W}{Z}"
    print(sep)
    print(f"  {B}TRENDRIDER v7{Z}  {now}  [{status}]")
    print(f"  BTC ${price:,.2f}  |  "
          f"Wallet {G}${usdt:.8f} USDT{Z}  {btc:.8f} BTC  Total ${total:.8f}")

    if ind:
        ar = '↑' if ind.get('fast', 0) > ind.get('slow', 0) else '↓'
        fc = G if ar == '↑' else R
        rc = G if ind['rsi'] < 40 else (R if ind['rsi'] > 65 else Z)
        vc = G if ind.get('vol', 0) >= P['volMin'] else R
        print(f"  EMA{fc}{ar}{Z} F:{ind['fast']:,.2f} S:{ind['slow']:,.2f} "
              f"T:{ind['trend']:,.2f}  "
              f"RSI {rc}{ind['rsi']:.8f}{Z}  "
              f"ATR ${ind['atr']:.8f}  ADX {ind['adx']:.8f}")
        sc = G if tside == 'BUY' else R
        print(f"  VolRatio {vc}{ind['vol']:.3f}×{Z}  "
              f"LastBTC {ind.get('last_vol',0):.8f}  "
              f"10BarAvg {ind.get('vol_avg',0):.8f}  "
              f"| Trade {sc}{tside}{Z} {tqty:.8f}BTC@${tpx:,.2f}")
    else:
        print(f"  Warming up… {bars}/55 candles needed")

    if pending:
        elapsed = time.time() - pending.get('placed_at', time.time())
        remain  = max(0, ORDER_TIMEOUT - elapsed)
        print(f"  {Y}⏳ PENDING BUY  @ ${pending['px']:,.2f}  "
              f"qty {pending['qty']:.6f}  ({remain:.0f}s remaining){Z}")

    if pos:
        qty  = pos.get('qty', 0.0)
        unr  = (price - pos['entry']) * qty
        uc   = G if unr >= 0 else R
        pt   = '✓partial' if pos.get('partialTaken') else '○partial'
        print(f"  {G}▶ {pos['setup']}{Z}  entry ${pos['entry']:,.2f}  "
              f"stop ${pos['stop']:,.2f}  target ${pos['target']:,.2f}  "
              f"bars {pos.get('barsHeld',0)}/{P['maxHoldBars']}  "
              f"PnL {uc}{unr:+.5f}{Z}  {pt}")
    elif not pending:
        print(f"  {Y}FLAT — {state.get('sig_msg','scanning…')}{Z}")

    nc = G if net >= 0 else R
    print(f"  Trades {len(trades)}  Wins {wins}  Net {nc}{net:+.5f}{Z}", end="")
    if trades:
        t  = trades[-1]
        tc = G if t['pnl'] > 0 else R
        print(f"   last [{t['setup']}] {t['outcome']} "
              f"${t['entry']:,.0f}→${t['exit']:,.0f} "
              f"{tc}{t['pnl']:+.5f}{Z}", end="")
    print()
    sys.stdout.flush()

def draw(state):
    if IS_TTY:
        _draw_tty(state)
    else:
        _draw_compact(state)

# ── Main ──────────────────────────────────────────────────────────────────────
def main():
    key = os.getenv('BINANCE_API_KEY')
    sec = os.getenv('BINANCE_SECRET_KEY')
    if not key or not sec:
        sys.exit("ERROR: BINANCE_API_KEY / BINANCE_SECRET_KEY missing from .env")

    client = Client(key, sec, tld='us')
    flt    = get_filters(client)

    print(f"{B}TRENDRIDER v7 starting…{Z}", flush=True)
    cancel_open_orders(client)
    btc, usdt = get_balances(client)
    print(f"Balance: ${usdt:.4f} USDT  {btc:.6f} BTC", flush=True)
    print(f"Filters: step={flt['step']}  tick={flt['tick']}  "
          f"min_notional=${flt['min_notional']}", flush=True)

    print("Fetching 200 1-min klines…", flush=True)
    raw     = client.get_klines(symbol=SYMBOL,
                                interval=Client.KLINE_INTERVAL_1MINUTE, limit=200)
    candles = deque(maxlen=300)
    for k in raw:
        candles.append({'time': k[0], 'open': float(k[1]), 'high': float(k[2]),
                        'low':  float(k[3]), 'close': float(k[4]), 'volume': float(k[5])})
    print(f"Loaded {len(candles)} candles — connecting WebSocket…", flush=True)
    time.sleep(2)

    # Detect existing BTC position and cost basis
    existing_entry = None
    if btc >= flt['min_qty']:
        try:
            my_trades = client.get_my_trades(symbol=SYMBOL, limit=50)
            bought = sold = cost = 0.0
            for t in reversed(my_trades):
                q = float(t['qty'])
                if t['isBuyer']:
                    bought += q; cost += q * float(t['price'])
                else:
                    sold += q
                if (bought - sold) >= btc * 0.95:
                    existing_entry = cost / bought if bought > 0 else None
                    break
        except BinanceAPIException:
            pass

    lock  = threading.Lock()
    state = {
        'price':         float(raw[-1][4]),
        'trade_qty':     0.0,
        'trade_px':      0.0,
        'trade_side':    '---',
        'btc':           btc,
        'usdt':          usdt,
        'pos':           None,
        'trades':        [],
        'ind':           {},
        'bars':          len(candles),
        'status':        'STARTING',
        'sig_msg':       'Scanning…',
        'pending_order': None,
    }

    if btc >= flt['min_qty'] and btc * float(raw[-1][4]) >= flt['min_notional']:
        ep = existing_entry or float(raw[-1][4])
        atr_est = 50.0
        state['pos'] = {
            'setup': 'EXISTING', 'direction': 'LONG',
            'entry': ep, 'qty': btc,
            'stop':      ep - P['atrStop']  * atr_est,
            'target':    ep + P['atrTp']    * atr_est,
            'partialAt': ep + P['partialAt']* atr_est,
            'atr': atr_est, 'rsi': 50.0, 'adx': 20.0, 'vol': 1.0,
            'barsHeld': 0, 'partialTaken': False,
        }
        print(f"{Y}Existing BTC detected — entry est. ${ep:,.2f}{Z}", flush=True)

    stop_event = threading.Event()
    last_bars  = [len(candles)]
    last_draw  = [0.0]

    def on_kline(msg):
        if msg.get('e') == 'error':
            return
        k = msg['k']
        with lock:
            state['price'] = float(k['c'])
            if k['x']:
                candles.append({
                    'time':   k['t'], 'open':   float(k['o']),
                    'high':   float(k['h']), 'low':    float(k['l']),
                    'close':  float(k['c']), 'volume': float(k['v']),
                })
                state['bars'] = len(candles)

    def on_trade(msg):
        if msg.get('e') == 'error':
            return
        with lock:
            state['trade_qty'] = float(msg['q'])   # BTC amount of this single trade
            state['trade_px']  = float(msg['p'])   # price it executed at
            state['trade_side'] = 'BUY' if not msg.get('m') else 'SELL'  # m=True → maker=seller

    twm = ThreadedWebsocketManager(api_key=key, api_secret=sec, tld='us')
    twm.start()
    twm.start_kline_socket(callback=on_kline, symbol=SYMBOL,
                           interval=Client.KLINE_INTERVAL_1MINUTE)
    twm.start_trade_socket(callback=on_trade, symbol=SYMBOL)

    last_bal_refresh = time.time()

    try:
        state['status'] = 'RUNNING'
        while not stop_event.is_set():
            time.sleep(0.25)

            with lock:
                price     = state['price']
                pos       = state['pos']
                bar_count = state['bars']
                clist     = list(candles)

            # Refresh balances every 30s
            if time.time() - last_bal_refresh > 30:
                try:
                    b, u = get_balances(client)
                    with lock: state['btc'] = b; state['usdt'] = u
                    last_bal_refresh = time.time()
                except BinanceAPIException:
                    pass

            # Recompute indicators
            if len(clist) >= 55:
                closes  = np.array([c['close']  for c in clist])
                volumes = np.array([c['volume'] for c in clist])
                idx     = len(clist) - 1
                vol_avg = float(np.mean(volumes[max(0, idx-10):idx]))
                vol_ratio = calc_vol_ratio(volumes)[idx]
                with lock:
                    state['ind'] = {
                        'fast':    calc_ema(closes, P['emaFast'])[idx],
                        'slow':    calc_ema(closes, P['emaSlow'])[idx],
                        'trend':   calc_ema(closes, P['emaTrend'])[idx],
                        'rsi':     calc_rsi(closes)[idx],
                        'atr':     calc_atr(clist)[idx],
                        'vol':     vol_ratio,
                        'adx':     calc_adx(clist)[idx],
                        'candles': len(clist),
                        'last_vol': float(volumes[idx]),   # last CLOSED bar BTC volume
                        'vol_avg':  vol_avg,               # 10-bar avg of closed bars
                    }

            # ── Position management ───────────────────────────────────────────
            if pos:
                riskUnit = abs(pos['entry'] - pos['stop'])
                pnlR     = (price - pos['entry']) / riskUnit if riskUnit > 0 else 0

                stop = pos['stop']
                if pnlR >= P['trailActivate']:
                    trail = price - pos['atr'] * P['trailAtr']
                    stop  = max(stop, trail)
                    with lock:
                        if state['pos']: state['pos']['stop'] = stop

                outcome = None
                if price <= stop:
                    outcome = 'STOPPED'
                elif price >= pos.get('target', float('inf')):
                    outcome = 'TARGET'
                elif pos.get('barsHeld', 0) >= P['maxHoldBars']:
                    outcome = 'TIMEOUT'

                if outcome:
                    qty = pos.get('qty', 0.0)
                    if qty >= flt['min_qty'] and qty * price >= flt['min_notional']:
                        with lock: state['status'] = f'SELLING ({outcome})'
                        fp, fq, via = sell_best(client, flt, qty, pos['entry'], outcome)
                        pnl = (fp - pos['entry']) * fq
                        with lock:
                            state['trades'].append({
                                'setup': pos['setup'], 'outcome': outcome,
                                'entry': pos['entry'], 'exit': fp,
                                'pnl': pnl, 'via': via,
                            })
                            state['pos']    = None
                            state['status'] = 'RUNNING'
                        b, u = get_balances(client)
                        with lock: state['btc'] = b; state['usdt'] = u

                elif not pos.get('partialTaken') and price >= pos.get('partialAt', float('inf')):
                    qty  = pos.get('qty', 0.0)
                    half = floor_qty(qty * 0.5, flt['step'])
                    if half >= flt['min_qty'] and half * price >= flt['min_notional']:
                        with lock: state['status'] = 'PARTIAL'
                        fp, fq, via = sell_best(client, flt, half, pos['entry'], 'PARTIAL')
                        pnl = (fp - pos['entry']) * fq
                        with lock:
                            if state['pos']:
                                state['pos']['qty']          -= fq
                                state['pos']['partialTaken'] = True
                            state['trades'].append({
                                'setup': pos['setup'], 'outcome': 'PARTIAL',
                                'entry': pos['entry'], 'exit': fp,
                                'pnl': pnl, 'via': via,
                            })
                            state['status'] = 'RUNNING'
                        b, u = get_balances(client)
                        with lock: state['btc'] = b; state['usdt'] = u

            # ── New bar: increment barsHeld + signal check ────────────────────
            if bar_count > last_bars[0]:
                last_bars[0] = bar_count
                with lock:
                    if state['pos']:
                        state['pos']['barsHeld'] = state['pos'].get('barsHeld', 0) + 1

                if not pos and not state.get('pending_order'):
                    sig, reason = get_signal(clist)
                    if sig:
                        with lock: u = state['usdt']
                        stop_dist = sig['atr'] * P['atrStop']
                        risk_qty  = (u * P['riskPct']) / stop_dist if stop_dist > 0 else 0
                        max_qty   = floor_qty(u * EQUITY_PCT / price, flt['step'])
                        qty       = floor_qty(min(risk_qty, max_qty), flt['step'])
                        notional  = qty * price

                        if qty >= flt['min_qty'] and notional >= flt['min_notional']:
                            with lock: state['status'] = 'BUYING'
                            bid, _ = best_bid_ask(client)
                            try:
                                o = client.create_order(
                                    symbol=SYMBOL, side='BUY', type='LIMIT_MAKER',
                                    quantity=fmt_qty(qty, flt['step']),
                                    price=fmt_px(bid, flt['tick']))
                                oid      = o['orderId']
                                placed   = time.time()
                                with lock:
                                    state['pending_order'] = {
                                        'px': bid, 'qty': qty,
                                        'oid': oid, 'placed_at': placed,
                                    }
                                deadline = placed + ORDER_TIMEOUT
                                filled   = False
                                while time.time() < deadline and not stop_event.is_set():
                                    time.sleep(0.25)
                                    o = client.get_order(symbol=SYMBOL, orderId=oid)
                                    if o['status'] == 'FILLED':
                                        fp  = float(o['cummulativeQuoteQty']) / float(o['executedQty'])
                                        fq  = float(o['executedQty'])
                                        a   = sig['atr']
                                        with lock:
                                            state['pos'] = {
                                                **sig,
                                                'entry':     fp,
                                                'stop':      fp - a * P['atrStop'],
                                                'target':    fp + a * P['atrTp'],
                                                'partialAt': fp + a * P['partialAt'],
                                                'qty':       fq,
                                            }
                                            state['pending_order'] = None
                                            state['status']        = 'RUNNING'
                                        b, u = get_balances(client)
                                        with lock: state['btc'] = b; state['usdt'] = u
                                        filled = True
                                        break
                                    with lock: p_now = state['price']
                                    if p_now > sig['target']:
                                        break
                                if not filled:
                                    try: client.cancel_order(symbol=SYMBOL, orderId=oid)
                                    except BinanceAPIException: pass
                                    with lock:
                                        state['pending_order'] = None
                                        state['status']        = 'RUNNING'
                            except BinanceAPIException as e:
                                print(f"{R}BUY err: {e.message}{Z}", flush=True)
                                with lock:
                                    state['pending_order'] = None
                                    state['status']        = 'RUNNING'
                        else:
                            low_funds = notional < flt['min_notional']
                            with lock:
                                state['sig_msg'] = (
                                    f"⚠ LOW FUNDS: ${notional:.2f} notional < "
                                    f"${flt['min_notional']:.0f} min — signal {sig['setup']} fired"
                                    if low_funds else
                                    f"Signal {sig['setup']} — qty too small")
                    else:
                        with lock:
                            state['sig_msg'] = f'FLAT — {reason}'

            # ── Draw dashboard ────────────────────────────────────────────────
            now = time.time()
            draw_interval = 0.25 if IS_TTY else 5.0
            if now - last_draw[0] >= draw_interval:
                last_draw[0] = now
                with lock:
                    snap = {**state,
                            'pos':           dict(state['pos']) if state['pos'] else None,
                            'trades':        list(state['trades']),
                            'ind':           dict(state['ind']),
                            'pending_order': dict(state['pending_order']) if state['pending_order'] else None}
                draw(snap)

    except KeyboardInterrupt:
        pass

    finally:
        stop_event.set()
        twm.stop()
        print(f"\n{Y}Shutting down…{Z}", flush=True)
        cancel_open_orders(client)
        btc, _ = get_balances(client)
        with lock:
            p   = state.get('price', 0)
            pos = state.get('pos')
        if btc >= flt['min_qty'] and btc * p >= flt['min_notional']:
            entry = pos['entry'] if pos else p
            print(f"{Y}Liquidating {btc:.6f} BTC…{Z}", flush=True)
            sell_best(client, flt, btc, entry, 'SHUTDOWN')

        trades = state.get('trades', [])
        wins   = sum(1 for t in trades if t['pnl'] > 0)
        net    = sum(t['pnl'] for t in trades)
        print(f"\n{C}{'═'*W}{Z}")
        print(f"  {B}SESSION SUMMARY{Z}  {len(trades)} trades  {wins} wins  "
              f"net {G if net>=0 else R}{net:+.5f}{Z}")
        print(f"{C}{'─'*W}{Z}")
        for t in trades:
            tc = G if t['pnl'] > 0 else R
            print(f"  [{t['setup']}] {t['outcome']}  "
                  f"${t['entry']:,.2f}→${t['exit']:,.2f}  "
                  f"{tc}{t['pnl']:+.5f}{Z}  via {t.get('via','?')}")
        print(f"{C}{'═'*W}{Z}")


if __name__ == '__main__':
    main()
