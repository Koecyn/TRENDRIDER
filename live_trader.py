#!/usr/bin/env python3
"""
live_trader.py — HFT Mean-Reversion, Binance.US
Long-only: maker limit buy at lower BB, sell BTC we own on recovery.
Strategy: BB(15,1.5σ) + RSI(14) + Momentum slope — 5-second bars.

Usage:
    export BINANCE_API_KEY="your_key"
    export BINANCE_SECRET_KEY="your_secret"
    python live_trader.py
"""

import os, sys, time, math, json
from datetime import datetime
from collections import deque
import numpy as np

try:
    from binance.client import Client
    from binance.exceptions import BinanceAPIException
except ImportError:
    print("pip install python-binance")
    sys.exit(1)

# ─────────────────────────────────────────────────────────────────────────────
# Strategy config — matched to backtest optimal params
# ─────────────────────────────────────────────────────────────────────────────
SYMBOL        = "BTCUSDT"
BAR_S         = 5          # 5-second bars
LIVE_WARMUP   = 15         # live bars required before trading (75 seconds)
EQUITY_PCT    = 0.95       # deploy 95% of free USDT per trade

BB_WINDOW     = 15         # 15 × 5s = 75 seconds
BB_NSTD       = 1.5        # 1.5σ bands  →  3:1 R:R with 0.5σ stop
RSI_PERIOD    = 14
MOM_FAST      = 3
MOM_SLOW      = 12

RSI_ENTRY     = 42.0       # oversold threshold (buy)
RSI_EXIT      = 58.0       # overbought threshold (sell)
BB_ENTRY_PCT  = 0.20       # enter when price in bottom 20% of BB range
BB_STOP_MULT  = 0.5        # stop = bb_lower - 0.5 * bb_std
MAX_HOLD_S    = 30         # time-stop: 30 seconds

ORDER_TIMEOUT = 8          # seconds to wait for maker fill before cancel

# Fees: maker = 0%, taker = 0.002% (Binance.US VIP)
MAKER_FEE     = 0.0
TAKER_FEE     = 0.00002   # 0.002% expressed as a fraction


# ─────────────────────────────────────────────────────────────────────────────
# Indicators — self-contained, causal, no external deps beyond numpy
# ─────────────────────────────────────────────────────────────────────────────

def _ema(arr: np.ndarray, span: int) -> np.ndarray:
    alpha = 2.0 / (span + 1)
    out = np.full(len(arr), np.nan)
    fv = np.where(~np.isnan(arr))[0]
    if not len(fv):
        return out
    out[fv[0]] = arr[fv[0]]
    for i in range(fv[0] + 1, len(arr)):
        v = arr[i] if not np.isnan(arr[i]) else out[i - 1]
        out[i] = alpha * v + (1 - alpha) * out[i - 1]
    return out


def calc_rsi(close: np.ndarray, period: int = 14) -> np.ndarray:
    delta    = np.diff(close, prepend=close[0])
    avg_gain = _ema(np.maximum(delta, 0),  2 * period - 1)
    avg_loss = _ema(np.maximum(-delta, 0), 2 * period - 1)
    rs       = avg_gain / (avg_loss + 1e-12)
    return 100.0 - 100.0 / (1.0 + rs)


def calc_bb(close: np.ndarray, window: int = 15, n_std: float = 1.5):
    """Returns (mid, lower, upper, pct_b, std)."""
    n   = len(close)
    mid = np.full(n, np.nan)
    std = np.full(n, np.nan)
    cs  = np.cumsum(close)
    for i in range(window - 1, n):
        sl     = close[i - window + 1 : i + 1]
        mid[i] = sl.mean()
        std[i] = sl.std()
    lower = mid - n_std * std
    upper = mid + n_std * std
    pct_b = (close - lower) / (upper - lower + 1e-12)
    return mid, lower, upper, pct_b, std


def calc_mom_slope(close: np.ndarray, fast: int = 3, slow: int = 12,
                   smooth: int = 2) -> np.ndarray:
    ema_f = _ema(close, fast)
    ema_s = _ema(close, slow)
    mom   = (ema_f - ema_s) / (close + 1e-12)
    return _ema(np.gradient(mom), smooth)


# ─────────────────────────────────────────────────────────────────────────────
# Exchange helpers
# ─────────────────────────────────────────────────────────────────────────────

def get_filters(client: Client, symbol: str) -> dict:
    info = client.get_symbol_info(symbol)
    f = {'min_notional': 10.0}
    for filt in info['filters']:
        ft = filt['filterType']
        if ft == 'LOT_SIZE':
            f['step_size'] = float(filt['stepSize'])
            f['min_qty']   = float(filt['minQty'])
        elif ft == 'PRICE_FILTER':
            f['tick_size'] = float(filt['tickSize'])
        elif ft in ('MIN_NOTIONAL', 'NOTIONAL'):
            f['min_notional'] = float(filt.get('minNotional', 10))
    return f


def floor_qty(qty: float, step: float) -> float:
    precision = max(0, round(-math.log10(step)))
    factor    = 10 ** precision
    return math.floor(qty * factor) / factor


def fmt_price(price: float, tick: float) -> str:
    precision = max(0, round(-math.log10(tick)))
    return f"{price:.{precision}f}"


def get_balances(client: Client, base: str, quote: str = 'USDT'):
    account = client.get_account()
    bal     = {b['asset']: float(b['free']) for b in account['balances']}
    return bal.get(base, 0.0), bal.get(quote, 0.0)


def best_bid_ask(client: Client, symbol: str):
    book = client.get_order_book(symbol=symbol, limit=5)
    bid  = float(book['bids'][0][0])
    ask  = float(book['asks'][0][0])
    return bid, ask


# ─────────────────────────────────────────────────────────────────────────────
# Terminal dashboard
# ─────────────────────────────────────────────────────────────────────────────

W = 64

def _bar(label: str, value: str) -> str:
    return f"  {label:<24}{value:>36}"


def render(st: dict) -> None:
    os.system('clear')
    now = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    print('═' * W)
    print(f"  HFT MEAN-REVERSION  |  {st['symbol']}  |  Binance.US")
    print(f"  {now}  |  Bar #{st['bar_count']}")
    print('═' * W)

    # ── Wallet ────────────────────────────────────────────────────────────
    price = st.get('price', 0.0)
    total = st['usdt_bal'] + st['btc_bal'] * price
    print(f"\n  WALLET")
    print(_bar("USDT (free):",     f"${st['usdt_bal']:>10.4f}"))
    print(_bar("BTC (free):",      f"{st['btc_bal']:>14.8f} BTC"))
    print(_bar("Total est. value:", f"${total:>10.4f}"))

    # ── Live indicators ───────────────────────────────────────────────────
    ind   = st.get('ind', {})
    warm  = st.get('warmup', True)
    label = f"LIVE DATA {'(WARMING UP — ' + str(st['live_bars']) + '/' + str(LIVE_WARMUP) + ' bars)' if warm else ''}"
    print(f"\n  {label}")
    print(_bar("Price:",       f"${price:>14.2f}"))
    print(_bar("BB Lower:",    f"${ind.get('bb_low', 0):>14.2f}"))
    print(_bar("BB Mid:",      f"${ind.get('bb_mid', 0):>14.2f}"))
    print(_bar("BB Upper:",    f"${ind.get('bb_up', 0):>14.2f}"))
    print(_bar("RSI:",         f"  {ind.get('rsi', 0):>12.2f}"))
    print(_bar("Mom Slope:",   f"  {ind.get('mom', 0):>+12.6f}"))
    print(_bar("BB Zone %:",   f"  {ind.get('bb_pct', 0)*100:>11.1f}%"))
    print(_bar("Signal:",      f"  {st.get('signal', '—'):>12}"))

    # ── Current position ──────────────────────────────────────────────────
    pos   = st.get('position')
    pend  = st.get('pending_order')
    print(f"\n  POSITION")
    if pos:
        held = time.time() - pos['entry_time']
        unr  = (price - pos['entry_price']) * pos['qty']
        sign = '+' if unr >= 0 else ''
        print(_bar("Status:",       "  LONG (open)"))
        print(_bar("Entry Price:",  f"${pos['entry_price']:>14.4f}"))
        print(_bar("Quantity:",     f"  {pos['qty']:>12.8f} BTC"))
        print(_bar("Stop Price:",   f"${pos['stop_price']:>14.4f}"))
        print(_bar("Hold Time:",    f"  {held:>8.0f}s / {MAX_HOLD_S}s"))
        print(_bar("Unrealized:",   f"  {sign}${abs(unr):>10.4f}"))
    elif pend:
        print(_bar("Status:", f"  PENDING BUY @ ${pend['price']:.2f}"))
        print(_bar("Qty:",    f"  {pend['qty']:.8f} BTC"))
    else:
        print(_bar("Status:", "  No open position"))

    # ── Session P&L ───────────────────────────────────────────────────────
    trades     = st.get('trades', [])
    wins       = [t for t in trades if t['pnl'] > 0]
    losses     = [t for t in trades if t['pnl'] <= 0]
    gross_pnl  = sum(t['pnl'] for t in trades)
    total_fees = sum(t.get('fee', 0.0) for t in trades)
    net_pnl    = gross_pnl - total_fees
    wr         = len(wins) / len(trades) * 100 if trades else 0.0
    print(f"\n  SESSION P&L  (started {st.get('session_start', '—')})")
    print(_bar("Total Trades:",   f"  {len(trades):>12}"))
    print(_bar("Wins / Losses:",  f"  {len(wins):>4} / {len(losses):<4}        "))
    print(_bar("Win Rate:",       f"  {wr:>11.1f}%"))
    print(_bar("Gross P&L:",      f"  +${gross_pnl:>9.4f}  " if gross_pnl >= 0
                                  else f"  -${abs(gross_pnl):>9.4f}  "))
    print(_bar("Fees Paid:",      f"  ${total_fees:>10.4f}"))
    net_sign = '+' if net_pnl >= 0 else '-'
    print(_bar("NET P&L:",        f"  {net_sign}${abs(net_pnl):>9.4f}"))

    # ── Recent trades ─────────────────────────────────────────────────────
    print(f"\n  RECENT TRADES (last 10)")
    hdr = f"  {'Time':<9} {'Entry':>10} {'Exit':>10} {'PnL':>9} {'Reason':<7} {'Result'}"
    print(hdr)
    print(f"  {'─' * (W - 4)}")
    for t in trades[-10:]:
        result = '✓ WIN' if t['pnl'] > 0 else '✗ LOSS'
        sign   = '+' if t['pnl'] >= 0 else ''
        print(f"  {t['exit_time']:<9} "
              f"${t['entry_price']:>9.2f} "
              f"${t['exit_price']:>9.2f} "
              f"{sign}${abs(t['pnl']):>7.4f} "
              f"{t['reason']:<7} {result}")
    print('═' * W)
    print("  Ctrl+C to stop")


# ─────────────────────────────────────────────────────────────────────────────
# Main loop
# ─────────────────────────────────────────────────────────────────────────────

def run_pure_maker_loop(symbol: str = "BTCUSDT") -> None:
    api_key    = os.getenv('BINANCE_API_KEY')
    api_secret = os.getenv('BINANCE_SECRET_KEY')
    if not api_key or not api_secret:
        print("ERROR: Set BINANCE_API_KEY and BINANCE_SECRET_KEY env vars.")
        sys.exit(1)

    client     = Client(api_key, api_secret, tld='us')
    base_asset = symbol.replace("USDT", "")

    print(f"Connecting to Binance.US ({symbol})…")
    flt          = get_filters(client, symbol)
    step_size    = flt['step_size']
    tick_size    = flt['tick_size']
    min_qty      = flt['min_qty']
    min_notional = flt.get('min_notional', 10.0)
    print(f"Filters: step={step_size} tick={tick_size} min_notional={min_notional}")

    # ── Pre-load indicator history from recent 1-min klines ───────────────
    print("Pre-loading indicator history from recent klines…")
    klines = client.get_klines(symbol=symbol,
                               interval=Client.KLINE_INTERVAL_1MINUTE,
                               limit=200)
    init_closes = np.array([float(k[4]) for k in klines])

    closes  = deque(init_closes.tolist(), maxlen=500)
    # Volumes not available perfectly at 5s; proxy with kline volume / 12
    init_vols = np.array([float(k[5]) / 12 for k in klines])
    volumes = deque(init_vols.tolist(), maxlen=500)

    # ── Initial balances ──────────────────────────────────────────────────
    btc_bal, usdt_bal = get_balances(client, base_asset)

    # ── State ─────────────────────────────────────────────────────────────
    st = {
        'symbol':        symbol,
        'bar_count':     len(init_closes),
        'live_bars':     0,
        'price':         init_closes[-1],
        'usdt_bal':      usdt_bal,
        'btc_bal':       btc_bal,
        'ind':           {},
        'signal':        '—',
        'warmup':        True,
        'position':      None,
        'pending_order': None,
        'trades':        [],
        'session_start': datetime.now().strftime('%H:%M:%S'),
    }

    # Pending order tracking
    pend_id   = None
    pend_time = None

    # Current bar being assembled
    bar_t   = time.time()
    bar_o   = bar_h = bar_l = bar_c = init_closes[-1]
    prev_px = init_closes[-1]

    render(st)

    while True:
        loop_t = time.time()
        try:
            # ── 1. Fetch latest price ──────────────────────────────────────
            ticker = client.get_symbol_ticker(symbol=symbol)
            price  = float(ticker['price'])
            st['price'] = price

            # Update current bar
            bar_h = max(bar_h, price)
            bar_l = min(bar_l, price)
            bar_c = price

            # ── 2. Close bar every BAR_S seconds ──────────────────────────
            if time.time() - bar_t >= BAR_S:
                closes.append(bar_c)
                volumes.append(max(abs(bar_c - bar_o) * 1000 + 1.0, 1.0))
                st['bar_count'] += 1
                st['live_bars'] = min(st['live_bars'] + 1, LIVE_WARMUP + 1)

                # Reset bar
                bar_t = time.time()
                bar_o = bar_h = bar_l = bar_c = price
                prev_px = price

                # ── 3. Compute indicators ──────────────────────────────────
                c_arr = np.array(closes)
                v_arr = np.array(volumes)

                if len(c_arr) >= max(BB_WINDOW, RSI_PERIOD, MOM_SLOW):
                    rsi_a = calc_rsi(c_arr, RSI_PERIOD)
                    bb_mid_a, bb_low_a, bb_up_a, bb_pct_a, bb_std_a = calc_bb(
                        c_arr, BB_WINDOW, BB_NSTD)
                    mom_a = calc_mom_slope(c_arr, MOM_FAST, MOM_SLOW)

                    ind = {
                        'rsi':    float(rsi_a[-1]),
                        'bb_mid': float(bb_mid_a[-1]),
                        'bb_low': float(bb_low_a[-1]),
                        'bb_up':  float(bb_up_a[-1]),
                        'bb_pct': float(bb_pct_a[-1]),
                        'bb_std': float(bb_std_a[-1]),
                        'mom':    float(mom_a[-1]),
                    }
                    st['ind'] = ind
                    st['warmup'] = st['live_bars'] < LIVE_WARMUP

                    # Determine signal
                    if (not st['warmup']
                            and not any(np.isnan(v) for v in ind.values())
                            and not st['position']
                            and not pend_id):
                        if (ind['bb_pct'] < BB_ENTRY_PCT
                                and ind['rsi'] < RSI_ENTRY
                                and ind['mom'] > 0.0):
                            st['signal'] = 'LONG ★'
                        else:
                            st['signal'] = '—'

                # Refresh balances every 10 bars
                if st['bar_count'] % 10 == 0:
                    btc_bal, usdt_bal = get_balances(client, base_asset)
                    st['btc_bal']  = btc_bal
                    st['usdt_bal'] = usdt_bal

            # ─────────────────────────────────────────────────────────────
            # 4. Order / position state machine
            # ─────────────────────────────────────────────────────────────
            ind = st.get('ind', {})
            pos = st.get('position')

            # A) Check pending maker buy order
            if pend_id and not pos:
                try:
                    order = client.get_order(symbol=symbol, orderId=pend_id)
                    status = order['status']

                    if status == 'FILLED':
                        ep  = float(order['price'])
                        qty = float(order['executedQty'])
                        stp = ep - BB_STOP_MULT * ind.get('bb_std', ep * 0.001)
                        st['position'] = {
                            'entry_price': ep,
                            'qty':         qty,
                            'stop_price':  stp,
                            'entry_time':  time.time(),
                        }
                        st['pending_order'] = None
                        st['signal']        = 'HOLDING'
                        pend_id = pend_time = None

                    elif status in ('CANCELED', 'REJECTED', 'EXPIRED'):
                        st['pending_order'] = None
                        st['signal']        = '—'
                        pend_id = pend_time = None

                    elif time.time() - pend_time > ORDER_TIMEOUT:
                        client.cancel_order(symbol=symbol, orderId=pend_id)
                        st['pending_order'] = None
                        st['signal']        = '—'
                        pend_id = pend_time = None

                except BinanceAPIException:
                    pass

            # B) Manage open position (sell BTC we own)
            elif pos:
                held  = time.time() - pos['entry_time']
                r     = ind.get('rsi', 50.0)
                bb_m  = ind.get('bb_mid', price)
                reason = None

                if price < pos['stop_price']:
                    reason = 'STOP'
                elif held >= MAX_HOLD_S:
                    reason = 'TIME'
                elif not np.isnan(r) and (price >= bb_m or r > RSI_EXIT):
                    reason = 'TARGET'

                if reason:
                    qty       = pos['qty']
                    exit_px   = price
                    fee       = 0.0

                    try:
                        if reason == 'TARGET':
                            # Maker limit sell at current ask — 0% fee
                            bid, ask = best_bid_ask(client, symbol)
                            sell_px  = float(fmt_price(ask, tick_size))
                            sell_ord = client.create_order(
                                symbol   = symbol,
                                side     = 'SELL',
                                type     = 'LIMIT_MAKER',
                                quantity = fmt_price(qty, step_size),
                                price    = fmt_price(sell_px, tick_size),
                            )
                            # Wait up to ORDER_TIMEOUT for fill
                            for _ in range(ORDER_TIMEOUT * 2):
                                time.sleep(0.5)
                                o = client.get_order(symbol=symbol,
                                                     orderId=sell_ord['orderId'])
                                if o['status'] == 'FILLED':
                                    exit_px = float(o['price'])
                                    fee     = 0.0   # maker = 0% fee
                                    break
                            else:
                                # Didn't fill → cancel and fall to market taker
                                try:
                                    client.cancel_order(symbol=symbol,
                                                        orderId=sell_ord['orderId'])
                                except BinanceAPIException:
                                    pass
                                reason = 'TARGET→MKT'

                        if reason in ('STOP', 'TIME', 'TARGET→MKT'):
                            mo    = client.order_market_sell(
                                symbol=symbol, quantity=fmt_price(qty, step_size))
                            fills = mo.get('fills', [])
                            if fills:
                                total_q  = sum(float(f['qty'])   for f in fills)
                                total_pq = sum(float(f['price']) * float(f['qty'])
                                              for f in fills)
                                exit_px  = total_pq / total_q
                                # Taker fee = 0.002% of notional
                                fee      = exit_px * qty * TAKER_FEE

                        pnl = (exit_px - pos['entry_price']) * qty
                        st['trades'].append({
                            'entry_price': pos['entry_price'],
                            'exit_price':  exit_px,
                            'qty':         qty,
                            'pnl':         pnl,
                            'fee':         fee,
                            'reason':      reason,
                            'exit_time':   datetime.now().strftime('%H:%M:%S'),
                        })
                        st['position'] = None
                        st['signal']   = '—'

                        # Refresh balance immediately after trade
                        btc_bal, usdt_bal = get_balances(client, base_asset)
                        st['btc_bal']  = btc_bal
                        st['usdt_bal'] = usdt_bal

                    except BinanceAPIException as e:
                        st['signal'] = f'EXIT ERR {e.status_code}'

            # C) Place new maker buy if signal active and not warming up
            elif (not pend_id
                    and st.get('signal') == 'LONG ★'
                    and not st.get('warmup', True)
                    and ind):

                usdt_free = st['usdt_bal']
                qty       = floor_qty(usdt_free * EQUITY_PCT / price, step_size)

                if qty >= min_qty and qty * price >= min_notional:
                    try:
                        bid, _ = best_bid_ask(client, symbol)
                        buy_px = float(fmt_price(bid, tick_size))
                        order  = client.create_order(
                            symbol   = symbol,
                            side     = 'BUY',
                            type     = 'LIMIT_MAKER',
                            quantity = fmt_price(qty, step_size),
                            price    = fmt_price(buy_px, tick_size),
                        )
                        pend_id   = order['orderId']
                        pend_time = time.time()
                        st['pending_order'] = {'price': buy_px, 'qty': qty}
                        st['signal']        = f'ORDER @ ${buy_px:.2f}'
                    except BinanceAPIException as e:
                        st['signal'] = f'ERR {e.status_code}'
                else:
                    st['signal'] = f'INSUF (need ${min_notional:.0f})'

            # ── 5. Render ──────────────────────────────────────────────────
            render(st)

        except BinanceAPIException as e:
            st['signal'] = f'API {e.status_code} — retrying…'
            render(st)
            time.sleep(5)
            continue

        except KeyboardInterrupt:
            break

        # Pace to ~1 second per loop
        elapsed = time.time() - loop_t
        time.sleep(max(0.0, 1.0 - elapsed))

    # ── Shutdown summary ──────────────────────────────────────────────────────
    print('\n' + '═' * W)
    print("  SESSION COMPLETE")
    print('═' * W)
    trades    = st.get('trades', [])
    gross_pnl = sum(t['pnl'] for t in trades)
    fees      = sum(t.get('fee', 0.0) for t in trades)
    wins      = sum(1 for t in trades if t['pnl'] > 0)
    wr        = wins / len(trades) * 100 if trades else 0
    print(f"  Trades     : {len(trades)}")
    print(f"  Win Rate   : {wr:.1f}%")
    print(f"  Gross P&L  : ${gross_pnl:+.4f}")
    print(f"  Fees Paid  : ${fees:.4f}")
    print(f"  Net P&L    : ${gross_pnl - fees:+.4f}")
    print('═' * W)


# ─────────────────────────────────────────────────────────────────────────────

if __name__ == "__main__":
    run_pure_maker_loop()
