# live_trader_p2.py  —  Part 2 of 2
# cat live_trader_p1.py live_trader_p2.py > live_trader.py

TICK_S = 0.25          # Binance.US exchange sample rate: 250 ms
TICKS_PER_BAR = int(BAR_S / TICK_S)   # 20 ticks per 5-second bar
W = 64                 # dashboard width

# ── Dashboard ─────────────────────────────────────────────────────────────────
def render(st):
    os.system('clear')
    price = st.get('price', 0.0)
    now   = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    print('═' * W)
    print(f"  HFT MEAN-REVERSION  |  {st['symbol']}  |  Binance.US")
    print(f"  {now}  |  Bar #{st['bar_count']}  |  tick 250ms")
    print('═' * W)

    total = st['usdt_bal'] + st['btc_bal'] * price
    print(f"\n  WALLET")
    print(f"  {'USDT Free':<24} ${st['usdt_bal']:>10.4f}")
    print(f"  {'BTC Free':<24}  {st['btc_bal']:>10.8f}")
    print(f"  {'Total Value':<24} ${total:>10.4f}")

    ind  = st.get('ind', {})
    warm = st.get('warmup', True)
    tag  = f" (WARMING {st['live_bars']}/{LIVE_WARMUP})" if warm else ""
    print(f"\n  LIVE DATA{tag}")
    print(f"  {'Price':<24} ${price:>12.2f}")
    print(f"  {'BB Lower':<24} ${ind.get('bb_low', 0):>12.2f}")
    print(f"  {'BB Mid':<24} ${ind.get('bb_mid', 0):>12.2f}")
    print(f"  {'BB Upper':<24} ${ind.get('bb_up', 0):>12.2f}")
    print(f"  {'RSI':<24}  {ind.get('rsi', 0):>11.2f}")
    print(f"  {'Mom Slope':<24}  {ind.get('mom', 0):>+11.6f}")
    print(f"  {'BB Zone %':<24}  {ind.get('bb_pct', 0) * 100:>10.1f}%")
    mstr = "UPTREND ✓" if ind.get('macro_up') else "sideways/down"
    print(f"  {'Macro':<24}  {mstr:>12}")
    print(f"  {'Signal':<24}  {st.get('signal', '—'):>12}")

    pos  = st.get('position')
    pend = st.get('pending_order')
    print(f"\n  POSITION")
    if pos:
        held = time.time() - pos['entry_time']
        unr  = (price - pos['entry_price']) * pos['qty']
        sign = '+' if unr >= 0 else ''
        print(f"  {'Status':<24}  {'LONG  (open)':>12}")
        print(f"  {'Entry Price':<24} ${pos['entry_price']:>12.4f}")
        print(f"  {'Quantity BTC':<24}  {pos['qty']:>12.8f}")
        print(f"  {'Stop Price':<24} ${pos['stop_price']:>12.4f}")
        print(f"  {'Hold Time':<24}  {held:>8.0f}s / {MAX_HOLD_S}s")
        print(f"  {'Unrealized P&L':<24}  {sign}${abs(unr):>10.4f}")
    elif pend:
        print(f"  PENDING LIMIT_MAKER BUY  @ ${pend['price']:.2f}"
              f"  qty={pend['qty']:.8f} BTC")
    else:
        print(f"  No open position")

    trades = st.get('trades', [])
    wins   = [t for t in trades if t['pnl'] > 0]
    gross  = sum(t['pnl'] for t in trades)
    fees   = sum(t.get('fee', 0.0) for t in trades)
    net    = gross - fees
    wr     = len(wins) / len(trades) * 100 if trades else 0.0
    print(f"\n  SESSION  (started {st.get('session_start', '—')})")
    print(f"  {'Trades':<24}  {len(trades):>12}")
    print(f"  {'Wins / Losses':<24}  {len(wins)} / {len(trades) - len(wins)}")
    print(f"  {'Win Rate':<24}  {wr:>11.1f}%")
    g = '+' if gross >= 0 else ''
    print(f"  {'Gross P&L':<24}  {g}${gross:>9.4f}")
    print(f"  {'Fees (taker only)':<24}  ${fees:>10.4f}")
    n = '+' if net >= 0 else ''
    print(f"  {'NET P&L':<24}  {n}${abs(net):>10.4f}")

    print(f"\n  LAST 10 TRADES")
    print(f"  {'Time':<9} {'Entry':>10} {'Exit':>10} {'Net PnL':>9} {'Why':<9} Result")
    print(f"  {'─' * (W - 4)}")
    for t in trades[-10:]:
        s = '+' if t['pnl'] >= 0 else ''
        r = '✓ WIN' if t['pnl'] > 0 else '✗ LOSS'
        print(f"  {t['exit_time']:<9} "
              f"${t['entry_price']:>9.2f} "
              f"${t['exit_price']:>9.2f} "
              f"{s}${abs(t['pnl'] - t.get('fee', 0)):>7.4f} "
              f"{t['reason']:<9} {r}")
    print('═' * W)
    print("  Ctrl+C to stop")


# ── Main loop — 250 ms ticks, 5-second bars ───────────────────────────────────
def run_pure_maker_loop(symbol="BTCUSDT"):
    api_key    = os.getenv('BINANCE_API_KEY')
    api_secret = os.getenv('BINANCE_SECRET_KEY')
    if not api_key or not api_secret:
        print("ERROR: set BINANCE_API_KEY and BINANCE_SECRET_KEY")
        sys.exit(1)

    # tld='us' → all calls go to api.binance.us (Binance.US, not Binance.com)
    client     = Client(api_key, api_secret, tld='us')
    base_asset = symbol.replace("USDT", "")

    print(f"Binance.US  {symbol}  connecting…")
    flt          = get_filters(client, symbol)
    step_size    = flt['step_size']
    tick_size    = flt['tick_size']
    min_qty      = flt['min_qty']
    min_notional = flt.get('min_notional', 10.0)
    print(f"step={step_size}  tick={tick_size}  min_notional=${min_notional}")

    # Pre-load 200 one-minute klines → instant indicator warmup
    print("Pre-loading history…")
    klines = client.get_klines(symbol=symbol,
                               interval=Client.KLINE_INTERVAL_1MINUTE,
                               limit=200)
    init_c = np.array([float(k[4]) for k in klines])
    closes  = deque(init_c.tolist(),            maxlen=1000)
    volumes = deque([float(k[5]) / 12          # vol per 5s bar ≈ 1min vol / 12
                     for k in klines],          maxlen=1000)

    btc_bal, usdt_bal = get_balances(client, base_asset)
    st = {
        'symbol':        symbol,
        'bar_count':     len(init_c),
        'live_bars':     0,
        'price':         float(init_c[-1]),
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

    pend_id = pend_time = None
    tick_count = 0                       # ticks in current bar
    bar_o = bar_h = bar_l = bar_c = float(init_c[-1])
    bar_vol = 0.0
    prev_px = float(init_c[-1])
    last_render = 0.0

    render(st)

    while True:
        tick_t = time.time()
        try:
            # ── 250 ms price tick ─────────────────────────────────────────
            price      = float(client.get_symbol_ticker(symbol=symbol)['price'])
            st['price'] = price
            bar_h       = max(bar_h, price)
            bar_l       = min(bar_l, price)
            bar_c       = price
            bar_vol    += abs(price - prev_px) * 500 + 1.0
            prev_px     = price
            tick_count += 1

            # ── Close 5-second bar every TICKS_PER_BAR ticks ─────────────
            if tick_count >= TICKS_PER_BAR:
                closes.append(bar_c)
                volumes.append(max(bar_vol, 1.0))
                st['bar_count'] += 1
                st['live_bars']  = min(st['live_bars'] + 1, LIVE_WARMUP + 1)
                tick_count = 0
                bar_o = bar_h = bar_l = bar_c = price
                bar_vol = 0.0

                c_arr = np.array(closes)
                need  = max(BB_WINDOW, RSI_PERIOD, MOM_SLOW)
                if len(c_arr) >= need:
                    bm, bl, bh, bp, bs = calc_bb(c_arr, BB_WINDOW, BB_NSTD)
                    ind = {
                        'rsi':      float(calc_rsi(c_arr, RSI_PERIOD)[-1]),
                        'bb_mid':   float(bm[-1]),
                        'bb_low':   float(bl[-1]),
                        'bb_up':    float(bh[-1]),
                        'bb_pct':   float(bp[-1]),
                        'bb_std':   float(bs[-1]),
                        'mom':      float(calc_mom_slope(c_arr,
                                                         MOM_FAST, MOM_SLOW)[-1]),
                        'macro_up': int(calc_macro_up(c_arr, SMA_WINDOW)),
                    }
                    st['ind']    = ind
                    st['warmup'] = st['live_bars'] < LIVE_WARMUP

                    bad = any(np.isnan(ind[k])
                              for k in ('rsi', 'bb_pct', 'mom'))
                    if (not st['warmup'] and not st['position']
                            and not pend_id and not bad):
                        if (ind['bb_pct']  < BB_ENTRY_PCT
                                and ind['rsi'] < RSI_ENTRY
                                and ind['mom'] > 0.0
                                and ind['macro_up'] == 1):
                            st['signal'] = 'LONG ★'
                        else:
                            st['signal'] = '—'

                if st['bar_count'] % 10 == 0:
                    btc_bal, usdt_bal = get_balances(client, base_asset)
                    st['btc_bal']  = btc_bal
                    st['usdt_bal'] = usdt_bal

            # ── State machine ─────────────────────────────────────────────
            ind = st.get('ind', {})
            pos = st.get('position')

            # A: pending maker buy — check fill / timeout
            if pend_id and not pos:
                try:
                    o = client.get_order(symbol=symbol, orderId=pend_id)
                    if o['status'] == 'FILLED':
                        ep  = float(o['price'])
                        qty = float(o['executedQty'])
                        stp = ep - BB_STOP_MULT * ind.get('bb_std', ep * 0.001)
                        st['position']      = {'entry_price': ep, 'qty': qty,
                                               'stop_price':  stp,
                                               'entry_time':  time.time()}
                        st['pending_order'] = None
                        st['signal']        = 'HOLDING'
                        pend_id = pend_time = None
                    elif o['status'] in ('CANCELED', 'REJECTED', 'EXPIRED'):
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

            # B: open position — sell BTC we own when conditions met
            elif pos:
                held   = time.time() - pos['entry_time']
                reason = None
                if price < pos['stop_price']:
                    reason = 'STOP'
                elif held >= MAX_HOLD_S:
                    reason = 'TIME'
                elif (ind and not np.isnan(ind.get('rsi', float('nan')))
                        and (price >= ind.get('bb_mid', price + 1)
                             or ind['rsi'] > RSI_EXIT)):
                    reason = 'TARGET'

                if reason:
                    qty     = pos['qty']
                    exit_px = price
                    fee     = 0.0
                    try:
                        if reason == 'TARGET':
                            # Maker limit sell at ask — 0% fee
                            _, ask  = best_bid_ask(client, symbol)
                            sell_o  = client.create_order(
                                symbol=symbol, side='SELL', type='LIMIT_MAKER',
                                quantity=fmt_qty(qty, step_size),
                                price=fmt_px(ask, tick_size))
                            for _ in range(ORDER_TIMEOUT * 4):   # 250ms sleep
                                time.sleep(TICK_S)
                                o = client.get_order(symbol=symbol,
                                                     orderId=sell_o['orderId'])
                                if o['status'] == 'FILLED':
                                    exit_px = float(o['price'])
                                    fee     = 0.0
                                    break
                            else:
                                try:
                                    client.cancel_order(symbol=symbol,
                                                        orderId=sell_o['orderId'])
                                except BinanceAPIException:
                                    pass
                                reason = 'TARGET→MKT'

                        if reason in ('STOP', 'TIME', 'TARGET→MKT'):
                            mo    = client.order_market_sell(
                                symbol=symbol,
                                quantity=fmt_qty(qty, step_size))
                            fills = mo.get('fills', [])
                            if fills:
                                tq      = sum(float(f['qty']) for f in fills)
                                exit_px = (sum(float(f['price']) * float(f['qty'])
                                              for f in fills) / tq)
                                fee     = exit_px * qty * TAKER_FEE

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
                        btc_bal, usdt_bal = get_balances(client, base_asset)
                        st['btc_bal']  = btc_bal
                        st['usdt_bal'] = usdt_bal

                    except BinanceAPIException as e:
                        st['signal'] = f'EXIT ERR {e.status_code}'

            # C: place maker buy when signal fires
            elif (not pend_id and st.get('signal') == 'LONG ★'
                    and not st.get('warmup', True) and ind):
                qty = floor_qty(st['usdt_bal'] * EQUITY_PCT / price, step_size)
                if qty >= min_qty and qty * price >= min_notional:
                    try:
                        bid, _ = best_bid_ask(client, symbol)
                        order  = client.create_order(
                            symbol=symbol, side='BUY', type='LIMIT_MAKER',
                            quantity=fmt_qty(qty, step_size),
                            price=fmt_px(bid, tick_size))
                        pend_id             = order['orderId']
                        pend_time           = time.time()
                        st['pending_order'] = {
                            'price': round_px(bid, tick_size), 'qty': qty}
                        st['signal'] = f"ORDER @ ${round_px(bid, tick_size):.2f}"
                    except BinanceAPIException as e:
                        st['signal'] = f'ERR {e.status_code}'
                else:
                    st['signal'] = f'INSUF (min ${min_notional:.0f})'

            # ── Render dashboard once per second ──────────────────────────
            now = time.time()
            if now - last_render >= 1.0:
                render(st)
                last_render = now

            # ── Pace to 250 ms per tick ───────────────────────────────────
            elapsed = time.time() - tick_t
            time.sleep(max(0.0, TICK_S - elapsed))

        except BinanceAPIException as e:
            st['signal'] = f'API {e.status_code} — retry…'
            render(st)
            time.sleep(5)
        except KeyboardInterrupt:
            break

    # ── Final summary ─────────────────────────────────────────────────────────
    print('\n' + '═' * W)
    trades = st.get('trades', [])
    gross  = sum(t['pnl'] for t in trades)
    fees   = sum(t.get('fee', 0.0) for t in trades)
    wins   = sum(1 for t in trades if t['pnl'] > 0)
    wr     = wins / len(trades) * 100 if trades else 0
    print(f"  Trades: {len(trades)}  WR: {wr:.1f}%  Net P&L: ${gross - fees:+.4f}")
    print('═' * W)


if __name__ == "__main__":
    run_pure_maker_loop()
