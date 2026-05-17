# live_trader_p2.py  —  Part 2 of 2
# cat live_trader_p1.py live_trader_p2.py > live_trader.py

TICK_S        = 0.25
TICKS_PER_BAR = int(BAR_S / TICK_S)
W             = 66
_SP           = '|/-\\'

G = '\033[92m'; R = '\033[91m'; Y = '\033[93m'
C = '\033[96m'; D = '\033[2m';  B = '\033[1m'; Z = '\033[0m'

def _c(s, col):
    return f'{col}{s}{Z}'


def _macro_up_1m(c1m):
    """Macro uptrend directly from 1-minute closes — works with 72+ bars."""
    if len(c1m) < 72:
        return 0
    sma = c1m[-60:].mean()
    v   = (_ema(c1m, 3)[-1] - _ema(c1m, 12)[-1]) / (c1m[-1] + 1e-12)
    return int(c1m[-1] > sma and v > MOM5M_THRESH)

# ── Dashboard ─────────────────────────────────────────────────────────────────
def render(st):
    sys.stdout.write('\033[2J\033[H')
    sys.stdout.flush()

    price = st.get('price', 0.0)
    ts    = datetime.now().strftime('%H:%M:%S')
    spin  = _SP[st.get('spin', 0) % len(_SP)]
    tick  = st.get('tick', 0)

    # ── Header ────────────────────────────────────────────────────────────
    print(_c('═' * W, C))
    print(f"  {_c('HFT MEAN-REVERSION', B)}  {st['symbol']}  "
          f"Binance.US  {spin}  {ts}")
    print(f"  bar #{st['bar_count']}  "
          f"tick {tick:02d}/{TICKS_PER_BAR}  "
          f"${price:,.2f}")
    print(_c('─' * W, C))

    # ── Status — one line that tells you exactly what the bot is doing ────
    warm = st.get('warmup', True)
    pos  = st.get('position')
    pend = st.get('pending_order')
    sig  = st.get('signal', '—')
    ind  = st.get('ind', {})

    if warm:
        n    = st.get('live_bars', 0)
        secs = (LIVE_WARMUP - n) * BAR_S
        print(_c(f"  ◌  WARMING UP   {n}/{LIVE_WARMUP} bars   "
                 f"~{secs}s until trading starts", Y))
    elif pos:
        held = time.time() - pos['entry_time']
        unr  = (price - pos['entry_price']) * pos['qty']
        col  = G if unr >= 0 else R
        sign = '+' if unr >= 0 else ''
        trail = pos.get('trail_stop', pos['stop_price'])
        peak  = pos.get('peak_price', price)
        print(_c(f"  ▶  IN POSITION   entry ${pos['entry_price']:,.2f}   "
                 f"peak ${peak:,.2f}   trail ${trail:,.2f}   "
                 f"P&L {sign}${abs(unr):.4f}   {held:.0f}s/{MAX_HOLD_S}s", col))
    elif pend:
        age = time.time() - st.get('pend_time', time.time())
        print(_c(f"  !! ORDER PENDING !!   @ ${pend['price']:,.2f}   "
                 f"qty {pend['qty']:.8f}   "
                 f"{age:.0f}s / {ORDER_TIMEOUT}s  "
                 f"(maker buy placed — waiting for fill)", B))
    elif sig == 'LONG ★':
        print(_c(f"  ★  SIGNAL FOUND — placing LIMIT_MAKER buy now…", G))
    else:
        regime    = ind.get('regime', 'UNKNOWN')
        entry_pct = REGIME_ENTRY.get(regime, 0)
        bl        = ind.get('bb_low', 0.0)
        bu        = ind.get('bb_up',  0.0)
        cp        = (price - bl) / (bu - bl + 1e-12) * 100
        rc        = REGIME_COLOR.get(regime, D)
        no_entry  = entry_pct is None
        if no_entry:
            print(_c(f"  ✋  {regime} — no long entries, waiting for regime change", R))
        else:
            bb_s = _c(f'BB {cp:.0f}%✓', G) if cp < entry_pct*100 else _c(f'BB {cp:.0f}%✗ need<{entry_pct*100:.0f}%', R)
            print(f"  ◉  {_c(regime, rc)}   {bb_s}")

    # ── Wallet ────────────────────────────────────────────────────────────
    total = st['usdt_bal'] + st['btc_bal'] * price
    print(f"\n  {_c('WALLET', B)}")
    print(f"  USDT ${st['usdt_bal']:>10.2f}   "
          f"BTC {st['btc_bal']:>12.8f}   "
          f"Total ${total:>10.2f}")

    # ── Indicators ────────────────────────────────────────────────────────
    if ind:
        regime = ind.get('regime', 'UNKNOWN')
        rc     = REGIME_COLOR.get(regime, D)
        ep     = REGIME_ENTRY.get(regime)
        xp     = REGIME_EXIT.get(regime, 0.5)
        bl     = ind.get('bb_low', 0); bm2 = ind.get('bb_mid', 0); bu = ind.get('bb_up', 0)
        bb_w   = bu - bl
        bb_p   = ind.get('bb_pct', 0) * 100
        rsi_v  = ind.get('rsi', 0)
        r_col  = G if rsi_v < 45 else (R if rsi_v > 65 else Z)
        m_str  = _c('macro ✓', G) if ind.get('macro_up') else _c('macro ✗', R)
        entry_str = f'<{ep*100:.0f}%' if ep else 'NONE'
        print(f"\n  {_c('MARKET', B)}  {_c(regime, rc)}   "
              f"entry {entry_str}  exit >{xp*100:.0f}%")
        print(f"  BB  ${bl:,.2f} / ${bm2:,.2f} / ${bu:,.2f}   "
              f"width ${bb_w:,.2f}   zone {bb_p:.0f}%")
        print(f"  RSI {_c(f'{rsi_v:.1f}', r_col)}   {m_str}")

    # ── Session summary ───────────────────────────────────────────────────
    trades = st.get('trades', [])
    wins   = sum(1 for t in trades if t['pnl'] > 0)
    gross  = sum(t['pnl'] for t in trades)
    fees   = sum(t.get('fee', 0.0) for t in trades)
    net    = gross - fees
    wr     = wins / len(trades) * 100 if trades else 0.0
    nc     = G if net >= 0 else R
    nsign  = '+' if net >= 0 else ''
    print(f"\n  {_c('SESSION', B)}  {st.get('session_start','—')}   "
          f"trades {len(trades)}   wins {wins}   "
          f"wr {wr:.0f}%   "
          f"net {_c(f'{nsign}${abs(net):.4f}', nc)}")

    # ── Trade history — newest first ──────────────────────────────────────
    if trades:
        print(f"\n  {_c('TRADES', B)} — newest first")
        print(f"  {'time':<9}  {'entry':>9}  {'exit':>9}  {'net':>8}  why")
        print(f"  {'─' * (W - 4)}")
        for t in reversed(trades[-20:]):
            net_t = t['pnl'] - t.get('fee', 0.0)
            col   = G if t['pnl'] > 0 else R
            tsign = '+' if net_t >= 0 else ''
            print(_c(f"  {t['exit_time']:<9}  "
                     f"${t['entry_price']:>8.2f}  "
                     f"${t['exit_price']:>8.2f}  "
                     f"{tsign}${abs(net_t):>6.4f}  "
                     f"{t['reason']}", col))

    print(_c('\n' + '─' * W, C))
    print("  Ctrl+C to stop")


# ── Best-effort sell: LIMIT_MAKER at ask, market fallback after window_s ─────
def sell_best(client, symbol, qty, step_size, tick_size, window_s=30):
    """Try maker sell at ask; fall back to market after window_s seconds.
    Returns (exit_price, qty_sold, method) where method is 'MAKER' or 'MKT'."""
    exit_px = None
    qty_sold = qty
    try:
        _, ask  = best_bid_ask(client, symbol)
        o       = client.create_order(
            symbol=symbol, side='SELL', type='LIMIT_MAKER',
            quantity=fmt_qty(qty, step_size),
            price=fmt_px(ask, tick_size))
        oid      = o['orderId']
        deadline = time.time() + window_s
        while time.time() < deadline:
            time.sleep(TICK_S)
            status = client.get_order(symbol=symbol, orderId=oid)
            if status['status'] == 'FILLED':
                fills    = status.get('fills', [])
                if fills:
                    tq      = sum(float(f['qty']) for f in fills)
                    exit_px = sum(float(f['price'])*float(f['qty']) for f in fills)/tq
                    qty_sold = tq
                else:
                    exit_px = float(status['price'])
                return exit_px, qty_sold, 'MAKER'
        # Window expired — cancel and fall through to market
        try: client.cancel_order(symbol=symbol, orderId=oid)
        except BinanceAPIException: pass
    except BinanceAPIException:
        pass
    # Market fallback
    mo    = client.order_market_sell(symbol=symbol, quantity=fmt_qty(qty, step_size))
    fills = mo.get('fills', [])
    if fills:
        tq      = sum(float(f['qty']) for f in fills)
        exit_px = sum(float(f['price'])*float(f['qty']) for f in fills)/tq
        qty_sold = tq
    return exit_px, qty_sold, 'MKT'


# ── Main loop — 250 ms ticks, 5-second bars ───────────────────────────────────
def run_pure_maker_loop(symbol="BTCUSDT"):
    load_dotenv()
    api_key    = os.getenv('BINANCE_API_KEY')
    api_secret = os.getenv('BINANCE_SECRET_KEY')
    if not api_key or not api_secret:
        print("ERROR: missing BINANCE_API_KEY / BINANCE_SECRET_KEY in .env")
        sys.exit(1)

    client     = Client(api_key, api_secret, tld='us')
    base_asset = symbol.replace("USDT", "")

    print(f"Binance.US  {symbol}  connecting…")
    flt          = get_filters(client, symbol)
    step_size    = flt['step_size']
    tick_size    = flt['tick_size']
    min_qty      = flt['min_qty']
    min_notional = flt.get('min_notional', 10.0)
    print(f"step={step_size}  tick={tick_size}  min_notional=${min_notional}")

    print("Pre-loading history…")
    klines = client.get_klines(symbol=symbol,
                               interval=Client.KLINE_INTERVAL_1MINUTE,
                               limit=200)
    init_c    = np.array([float(k[4]) for k in klines])
    init_h    = np.array([float(k[2]) for k in klines])
    init_l    = np.array([float(k[3]) for k in klines])
    closes    = deque(init_c.tolist(), maxlen=1000)
    highs     = deque(init_h.tolist(), maxlen=1000)
    lows      = deque(init_l.tolist(), maxlen=1000)
    volumes   = deque([float(k[5]) / 12 for k in klines], maxlen=1000)
    closes_1m = deque(init_c.tolist(), maxlen=500)

    # ── Startup: cancel any open orders left in the book ─────────────────────
    print("Checking open orders…")
    try:
        open_orders = client.get_open_orders(symbol=symbol)
        for o in open_orders:
            client.cancel_order(symbol=symbol, orderId=o['orderId'])
            print(f"  Cancelled open order #{o['orderId']} "
                  f"@ ${float(o.get('price',0)):,.2f}")
        if open_orders:
            print(f"  Cleared {len(open_orders)} order(s)")
        else:
            print("  No open orders")
    except BinanceAPIException as e:
        print(f"  Order check error: {e.message}")

    btc_bal, usdt_bal = get_balances(client, base_asset)

    # ── Startup: detect existing BTC position and estimate cost basis ─────────
    existing_pos = None
    if btc_bal >= flt['min_qty']:
        print(f"  Existing BTC detected: {btc_bal:.8f} — finding cost basis…")
        try:
            my_trades = client.get_my_trades(symbol=symbol, limit=200)
            # Walk trades newest-first, accumulate buys until we account for btc_bal
            remaining = btc_bal
            cost = 0.0
            for t in reversed(my_trades):
                if remaining <= 0:
                    break
                if t['isBuyer']:
                    tq = min(float(t['qty']), remaining)
                    cost += float(t['price']) * tq
                    remaining -= tq
                else:
                    remaining += float(t['qty'])  # a sell means we owned more before
            avg_cost = cost / (btc_bal - max(remaining, 0) + 1e-12)
            print(f"  Avg cost basis: ${avg_cost:,.2f}")
        except BinanceAPIException:
            avg_cost = float(init_c[-1])
            print(f"  Could not fetch trades — using last price ${avg_cost:,.2f}")
        stp = avg_cost * 0.997
        existing_pos = {
            'entry_price': avg_cost, 'qty':        btc_bal,
            'stop_price':  stp,      'trail_stop':  stp,
            'peak_price':  avg_cost, 'stop_dist':   avg_cost * 0.001,
            'entry_time':  time.time(),
        }

    st = {
        'symbol':        symbol,
        'bar_count':     len(init_c),
        'live_bars':     0,
        'tick':          0,
        'spin':          0,
        'price':         float(init_c[-1]),
        'usdt_bal':      usdt_bal,
        'btc_bal':       btc_bal,
        'ind':           {},
        'signal':        '—',
        'warmup':        True,
        'position':      existing_pos,
        'pending_order': None,
        'pend_time':     None,
        'trades':        [],
        'session_start': datetime.now().strftime('%H:%M:%S'),
    }

    pend_id    = None
    tick_count = 0
    bar_h = bar_l = bar_c = float(init_c[-1])
    bar_vol = 0.0
    prev_px = float(init_c[-1])
    last_render = 0.0

    event_log = deque(maxlen=5000)

    def _log(msg):
        ts = datetime.now().strftime('%H:%M:%S.%f')[:-3]
        event_log.append(f"{ts}  {msg}")

    render(st)

    while True:
        tick_t = time.time()
        try:
            # ── 250 ms price tick ─────────────────────────────────────────
            price       = float(client.get_symbol_ticker(symbol=symbol)['price'])
            st['price'] = price
            bar_h       = max(bar_h, price)
            bar_l       = min(bar_l, price)
            bar_c       = price
            bar_vol    += abs(price - prev_px) * 500 + 1.0
            prev_px     = price
            tick_count += 1
            st['tick']  = tick_count

            # ── Close 5-second bar every TICKS_PER_BAR ticks ─────────────
            if tick_count >= TICKS_PER_BAR:
                closes.append(bar_c)
                highs.append(bar_h)
                lows.append(bar_l)
                volumes.append(max(bar_vol, 1.0))
                st['bar_count'] += 1
                st['live_bars']  = min(st['live_bars'] + 1, LIVE_WARMUP + 1)
                tick_count = 0
                st['tick'] = 0
                bar_h = bar_l = bar_c = price
                bar_vol = 0.0

                # Append a 1-minute close every 12 five-second bars
                if st['live_bars'] % 12 == 0:
                    closes_1m.append(bar_c)

                if st['bar_count'] % 10 == 0:
                    btc_bal, usdt_bal = get_balances(client, base_asset)
                    st['btc_bal']  = btc_bal
                    st['usdt_bal'] = usdt_bal

            # ── All indicators recomputed every 250ms from live data ─────
            # Append current live price to closed bars so every tick is fresh.
            # regime/macro use closes_1m for the broader 1-min structure.
            c5  = np.append(np.array(closes), price)
            c1m = np.array(closes_1m)
            if len(c5) >= max(BB_WINDOW, RSI_PERIOD, MOM_SLOW):
                bm, bl, bh, bp, bs = calc_bb(c5, BB_WINDOW, BB_NSTD)
                rsi_v  = float(calc_rsi(c5, RSI_PERIOD)[-1])
                mom_v  = float(calc_mom_slope(c5, MOM_FAST, MOM_SLOW)[-1])
                bb_pct = float(bp[-1])
                regime = classify_regime(closes_1m) if len(c1m) >= 20 else 'UNKNOWN'
                mac_v  = _macro_up_1m(c1m)
                st['ind'] = {
                    'rsi':      rsi_v,
                    'bb_mid':   float(bm[-1]),
                    'bb_low':   float(bl[-1]),
                    'bb_up':    float(bh[-1]),
                    'bb_pct':   bb_pct,
                    'bb_std':   float(bs[-1]),
                    'mom':      mom_v,
                    'macro_up': mac_v,
                    'regime':   regime,
                }
                st['warmup'] = st['live_bars'] < LIVE_WARMUP
                if tick_count == 1 and st.get('live_bars', 0) % 12 == 0:
                    _log(f"BAR  ${price:,.2f}  {regime:<10}  BB {bb_pct*100:5.1f}%  "
                         f"RSI {rsi_v:5.1f}  mom {mom_v:+.2e}  "
                         f"macro {'✓' if mac_v else '✗'}  "
                         f"width ${float(bh[-1]-bl[-1]):,.2f}")

            # ── Per-tick signal eval using live indicators ────────────────
            ind = st.get('ind', {})
            if (ind and not st.get('warmup', True)
                    and not st.get('position') and not pend_id):
                regime    = ind.get('regime', 'UNKNOWN')
                entry_pct = REGIME_ENTRY.get(regime)
                bb_low    = ind.get('bb_low', 0.0)
                bb_up     = ind.get('bb_up',  0.0)
                curr_pct  = (price - bb_low) / (bb_up - bb_low + 1e-12)
                # Uptrend also requires macro confirmation; other regimes just use bb_pct
                macro_ok  = (ind.get('macro_up') == 1) if regime == 'UPTREND' else True
                if (entry_pct is not None
                        and curr_pct < entry_pct
                        and macro_ok):
                    if st['signal'] != 'LONG ★':
                        _log(f"★ SIGNAL  ${price:,.2f}  {regime}  BB {curr_pct*100:.1f}%"
                             f"  (need <{entry_pct*100:.0f}%)")
                    st['signal'] = 'LONG ★'
                else:
                    st['signal'] = '—'

            # ── Trail stop: update high watermark every tick ──────────────
            if st.get('position'):
                p = st['position']
                p['peak_price'] = max(p.get('peak_price', price), price)
                new_trail = p['peak_price'] - p.get('stop_dist', p['peak_price'] * 0.001)
                p['trail_stop'] = max(p.get('trail_stop', p['stop_price']), new_trail)

            # ── State machine ─────────────────────────────────────────────
            ind = st.get('ind', {})
            pos = st.get('position')

            # A: pending maker buy — standing limit at zone top, cancel on zone exit
            if pend_id and not pos:
                pend_age     = time.time() - st.get('pend_time', time.time())
                bb_low       = ind.get('bb_low', 0)
                bb_up        = ind.get('bb_up', 1)
                entry_pct    = REGIME_ENTRY.get(ind.get('regime', 'UNKNOWN'), 0.5) or 0.5
                zone_top     = bb_low + entry_pct * (bb_up - bb_low)
                still_valid  = ind and price < zone_top
                try:
                    o = client.get_order(symbol=symbol, orderId=pend_id)
                    if o['status'] == 'FILLED':
                        ep    = float(o['price']); qty = float(o['executedQty'])
                        stp   = ep - BB_STOP_MULT * ind.get('bb_std', ep * 0.001)
                        sdist = max(ep - stp, ep * 0.0002)
                        st['position'] = {
                            'entry_price': ep,  'qty':        qty,
                            'stop_price':  stp, 'trail_stop': stp,
                            'peak_price':  ep,  'stop_dist':  sdist,
                            'entry_time':  time.time(),
                        }
                        st['pending_order'] = None; st['pend_time'] = None
                        st['signal'] = 'HOLDING'; pend_id = None
                        btc_bal, usdt_bal = get_balances(client, base_asset)
                        st['btc_bal'] = btc_bal; st['usdt_bal'] = usdt_bal
                        _log(f"✓ FILLED  maker  ${ep:,.2f}  qty {qty}  stop ${stp:,.2f}")
                    elif (not still_valid or pend_age > ORDER_TIMEOUT
                            or o['status'] in ('CANCELED', 'REJECTED', 'EXPIRED')):
                        try: client.cancel_order(symbol=symbol, orderId=pend_id)
                        except BinanceAPIException: pass
                        reason = ('timeout' if pend_age > ORDER_TIMEOUT
                                  else 'zone exit' if not still_valid
                                  else o['status'])
                        _log(f"✗ ORDER cancelled ({reason})  #{pend_id}  "
                             f"zone_top ${zone_top:,.2f}  price ${price:,.2f}")
                        st['pending_order'] = None; st['pend_time'] = None
                        st['signal'] = '—'; pend_id = None
                except BinanceAPIException:
                    pass

            # B: open position — sell BTC when exit conditions met
            elif pos:
                held   = time.time() - pos['entry_time']
                reason = None
                trail  = pos.get('trail_stop', pos['stop_price'])
                if price < trail:
                    # Below trail stop — profitable exit if above entry, else stop
                    reason = 'TRAIL' if price >= pos['entry_price'] else 'STOP'
                elif held >= MAX_HOLD_S:
                    reason = 'TIME'
                elif (ind
                        and ind.get('rsi', 0) > RSI_EXIT
                        and ind.get('mom', 1) < 0):
                    reason = 'MOM↓'   # RSI extended AND momentum flipped negative

                if reason:
                    _log(f"→ EXIT {reason}  held {held:.0f}s  "
                         f"entry ${pos['entry_price']:,.2f}  "
                         f"trail ${pos.get('trail_stop', pos['stop_price']):,.2f}  "
                         f"price ${price:,.2f}")
                    qty = pos['qty']
                    try:
                        exit_px, qty_sold, method = sell_best(
                            client, symbol, qty, step_size, tick_size, window_s=30)
                        fee = exit_px * qty_sold * (MAKER_FEE if method == 'MAKER' else TAKER_FEE)
                        pnl = (exit_px - pos['entry_price']) * qty_sold
                        _log(f"  ↳ sold  ${exit_px:,.2f} via {method}  "
                             f"PnL {'+' if pnl>=0 else ''}{pnl:.5f}  fee {fee:.5f}")
                        st['trades'].append({
                            'entry_price': pos['entry_price'],
                            'exit_price':  exit_px,
                            'qty':         qty_sold,
                            'pnl':         pnl,
                            'fee':         fee,
                            'reason':      f'{reason}({method})',
                            'exit_time':   datetime.now().strftime('%H:%M:%S'),
                        })
                        st['position'] = None
                        st['signal']   = '—'
                        btc_bal, usdt_bal    = get_balances(client, base_asset)
                        st['btc_bal']  = btc_bal
                        st['usdt_bal'] = usdt_bal

                    except BinanceAPIException as e:
                        _log(f"  ✗ EXIT ERR {e.status_code}: {e.message}")
                        st['signal'] = f'EXIT ERR {e.status_code}'

            # C: LIMIT_MAKER priced via ATR downward component
            # limit = price - (ATR × down_ratio) + small buffer
            # stays within entry zone; maker-only, 0% fee
            elif (not pend_id and st.get('signal') == 'LONG ★'
                    and not st.get('warmup', True) and ind):
                qty = floor_qty(st['usdt_bal'] * EQUITY_PCT / price, step_size)
                bb_low   = ind.get('bb_low', price)
                bb_up    = ind.get('bb_up',  price)
                ep_ratio = REGIME_ENTRY.get(ind.get('regime', 'UNKNOWN'), 0.5) or 0.5
                zone_top = bb_low + ep_ratio * (bb_up - bb_low)
                # ATR-based depth: how far down price is likely to travel
                atr, down_ratio = calc_atr(highs, lows, closes)
                expected_down   = atr * down_ratio          # e.g. $10 × 0.7 = $7
                buffer          = atr * 0.10                # 10% ATR buffer above target
                limit_px = round_px(
                    min(zone_top, price - expected_down + buffer), tick_size)
                if qty >= min_qty and qty * limit_px >= min_notional:
                    try:
                        order  = client.create_order(
                            symbol=symbol, side='BUY', type='LIMIT_MAKER',
                            quantity=fmt_qty(qty, step_size),
                            price=fmt_px(limit_px, tick_size))
                        pend_id             = order['orderId']
                        st['pend_time']     = time.time()
                        st['pending_order'] = {'price': limit_px, 'qty': qty}
                        st['signal']        = f"ORDER @ ${limit_px:,.2f}"
                        _log(f"▶ ORDER  maker  #{pend_id}  @ ${limit_px:,.2f}  "
                             f"ATR ${atr:.2f}  down_ratio {down_ratio:.2f}  "
                             f"expected_down ${expected_down:.2f}  "
                             f"zone_top ${zone_top:,.2f}  qty {qty}")
                    except BinanceAPIException as e:
                        _log(f"✗ ORDER ERR {e.status_code}: {e.message}")
                        st['signal'] = f'ERR {e.status_code}'
                else:
                    _log(f"✗ INSUF  usdt ${st['usdt_bal']:.2f}  qty {qty}")
                    st['signal'] = f'INSUF'

            # ── Render every 250 ms tick ──────────────────────────────────
            now = time.time()
            if now - last_render >= TICK_S:
                st['spin'] = (st.get('spin', 0) + 1) % len(_SP)
                render(st)
                last_render = now

            elapsed = time.time() - tick_t
            time.sleep(max(0.0, TICK_S - elapsed))

        except BinanceAPIException as e:
            _log(f"API ERR {e.status_code}: {e.message} — retry in 5s")
            st['signal'] = f'API {e.status_code} — retry…'
            render(st)
            time.sleep(5)
        except KeyboardInterrupt:
            break

    # ── Shutdown: cancel all open orders ─────────────────────────────────────
    print(f"\n{_c('Shutting down…', Y)}")
    try:
        open_orders = client.get_open_orders(symbol=symbol)
        for o in open_orders:
            client.cancel_order(symbol=symbol, orderId=o['orderId'])
            _log(f"SHUTDOWN: cancelled order #{o['orderId']} "
                 f"@ ${float(o.get('price',0)):,.2f}")
            print(f"  Cancelled order #{o['orderId']}")
    except BinanceAPIException as e:
        print(f"  Cancel error: {e.message}")

    # ── Shutdown: liquidate BTC — maker first, market after 30s ─────────────
    btc_bal, usdt_bal = get_balances(client, base_asset)
    if btc_bal >= flt['min_qty']:
        print(f"  Selling {btc_bal:.8f} BTC (30s maker window then market)…")
        try:
            exit_px, qty_sold, method = sell_best(
                client, symbol, btc_bal, step_size, tick_size, window_s=30)
            if exit_px:
                fee = exit_px * qty_sold * (MAKER_FEE if method == 'MAKER' else TAKER_FEE)
                pos = st.get('position')
                pnl = (exit_px - pos['entry_price']) * qty_sold if pos else 0.0
                col = G if pnl >= 0 else R
                print(_c(f"  Sold {qty_sold:.8f} BTC @ ${exit_px:,.2f} "
                         f"via {method}  PnL {'+' if pnl>=0 else ''}{pnl:.5f}", col))
                _log(f"SHUTDOWN: sold {qty_sold:.8f} BTC @ ${exit_px:,.2f} "
                     f"via {method}  PnL {'+' if pnl>=0 else ''}{pnl:.5f}")
                if pos:
                    st['trades'].append({
                        'entry_price': pos['entry_price'], 'exit_price': exit_px,
                        'qty': qty_sold, 'pnl': pnl, 'fee': fee,
                        'reason': f'SHUTDOWN({method})',
                        'exit_time': datetime.now().strftime('%H:%M:%S'),
                    })
        except BinanceAPIException as e:
            print(f"  Sell error: {e.message}")

    # ── Final summary ─────────────────────────────────────────────────────────
    trades = st.get('trades', [])
    gross  = sum(t['pnl'] for t in trades)
    fees   = sum(t.get('fee', 0.0) for t in trades)
    wins   = sum(1 for t in trades if t['pnl'] > 0)
    wr     = wins / len(trades) * 100 if trades else 0
    print(f"\n{_c('═'*W, C)}")
    print(f"  Trades {len(trades)}   WR {wr:.1f}%   "
          f"Net P&L {_c(f'{gross-fees:+.4f}', G if gross>=fees else R)}")
    print(_c('═' * W, C))

    # ── Full scroll log ───────────────────────────────────────────────────────
    if event_log:
        print(f"\n{_c('═'*W, C)}")
        print(f"  {_c('FULL SESSION LOG', B)}  —  {len(event_log)} entries")
        print(_c('─' * W, C))
        for line in event_log:
            # colour key event types
            if '★' in line or 'SIGNAL' in line:
                print(_c(f"  {line}", G))
            elif '▶ ORDER' in line or '✓ FILLED' in line:
                print(_c(f"  {line}", C))
            elif '→ EXIT' in line or '↳ sold' in line:
                col = G if '+' in line else R
                print(_c(f"  {line}", col))
            elif 'ERR' in line or '✗' in line:
                print(_c(f"  {line}", R))
            else:
                print(f"  {line}")
        print(_c('═' * W, C))


if __name__ == "__main__":
    run_pure_maker_loop()
