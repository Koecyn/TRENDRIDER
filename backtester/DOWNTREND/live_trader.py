#!/usr/bin/env python3
# ═══════════════════════════════════════════════════════════════════════════════
# SECTION 1 — Imports, Constants, Parameters
# ═══════════════════════════════════════════════════════════════════════════════
"""
TRENDRIDER v7 — DOWNTREND Regime Live Trader
Maker-only, long-only, no margin, no shorts. Starts with $100.
Exchange : Binance.US  (maker 0%, taker 0.02%)
Default  : paper mode (--live to enable real orders)

Run:
  python backtester/DOWNTREND/live_trader.py            # paper
  python backtester/DOWNTREND/live_trader.py --live     # real orders

Requirements:
  pip install ccxt[pro] numpy
"""

import os, sys, time, json, asyncio, argparse, logging
from collections import deque
from datetime import datetime, timezone

import numpy as np

# ── Parameters (must match winning backtest) ─────────────────────────────────
P = {
    'emaFast':       5,
    'emaSlow':       13,
    'emaTrend':      50,
    'emaMacro':      200,
    'emaSlopeN':     240,    # EMA200 slope lookback bars
    'rsiOB':         50.0,   # EMA_CROSS gate (requires RSI < 50)
    'rsiOS':         28,
    'atrStop':       1.0,    # × 1-min downside ATR
    'atrTp':         2.8,    # × N-bar rolling range
    'partialAt':     3.0,
    'trailAtr':      0.65,
    'trailActivate': 0.25,
    'maxHoldBars':   60,
    'adxMin':        22,
    'minBias':       0.05,
    'minBias60m':    0.0,
    'targetWindow':  10,
}

# ── Exchange / safety constants ───────────────────────────────────────────────
SYMBOL        = 'BTC/USDT'          # Binance.US pair
TAKER_FEE     = 0.0002              # 0.02%  — min profit to allow market sell
MAKER_FEE     = 0.0000              # 0%     — limit buys/sells are free
MIN_NOTIONAL  = 10.0                # Binance.US minimum order value USD
WARMUP_BARS   = 250                 # bars needed before indicators are stable
LOG_LEVEL     = logging.INFO

logging.basicConfig(
    level=LOG_LEVEL,
    format='%(asctime)s [%(levelname)s] %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S',
)
log = logging.getLogger('trendrider')

# ═══════════════════════════════════════════════════════════════════════════════
# SECTION 2 — Indicator Functions (exact match to strategy_downtrend.py)
# ═══════════════════════════════════════════════════════════════════════════════

def calc_ema(arr: np.ndarray, period: int) -> np.ndarray:
    out = np.zeros(len(arr))
    if len(arr) < period:
        return out
    k = 2.0 / (period + 1)
    out[period - 1] = np.mean(arr[:period])
    for i in range(period, len(arr)):
        out[i] = arr[i] * k + out[i-1] * (1 - k)
    return out

def calc_rsi(closes: np.ndarray, period: int = 14) -> np.ndarray:
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

def calc_atr(high: np.ndarray, low: np.ndarray,
             close: np.ndarray, period: int = 14) -> np.ndarray:
    n = len(close)
    out = np.zeros(n)
    if n < 2:
        return out
    trs = np.array([
        max(high[i]-low[i], abs(high[i]-close[i-1]), abs(low[i]-close[i-1]))
        for i in range(1, n)
    ])
    if len(trs) < period:
        return out
    out[period] = np.mean(trs[:period])
    for i in range(period, len(trs)):
        out[i+1] = (out[i] * (period-1) + trs[i]) / period
    return out

def calc_atr_bias(high: np.ndarray, low: np.ndarray,
                  close: np.ndarray, period: int = 14) -> np.ndarray:
    """Directional ATR bias normalised to [-1, +1]."""
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

def calc_range_mtf(high: np.ndarray, low: np.ndarray,
                   window: int = 5, smooth: int = 14) -> np.ndarray:
    """Rolling N-bar high-low range, Wilder-smoothed."""
    n = len(high)
    raw = np.zeros(n)
    for i in range(window - 1, n):
        raw[i] = max(high[i-window+1:i+1]) - min(low[i-window+1:i+1])
    out = np.zeros(n)
    start = window - 1 + smooth
    if start >= n:
        return out
    out[start] = np.mean(raw[window-1:start+1])
    for i in range(start + 1, n):
        out[i] = (out[i-1] * (smooth-1) + raw[i]) / smooth
    return out

def calc_adx(high: np.ndarray, low: np.ndarray,
             close: np.ndarray, period: int = 14) -> np.ndarray:
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

# ═══════════════════════════════════════════════════════════════════════════════
# SECTION 3 — Signal Engine (exact logic from strategy_downtrend.py)
# ═══════════════════════════════════════════════════════════════════════════════

def compute_indicators(highs, lows, closes):
    """Compute all indicators from numpy arrays; return dict of latest values."""
    h = np.array(highs, dtype=float)
    l = np.array(lows,  dtype=float)
    c = np.array(closes, dtype=float)

    ema5   = calc_ema(c, P['emaFast'])
    ema13  = calc_ema(c, P['emaSlow'])
    ema50  = calc_ema(c, P['emaTrend'])
    ema200 = calc_ema(c, P['emaMacro'])
    rsi    = calc_rsi(c, 14)
    atr_1m = calc_atr(h, l, c, 14)
    bias1m = calc_atr_bias(h, l, c, 14)
    bias60 = calc_atr_bias(h, l, c, 60)
    rng    = calc_range_mtf(h, l, P['targetWindow'], 14)
    adx    = calc_adx(h, l, c, 14)

    # EMA200 slope gate: compare current vs emaSlopeN bars ago
    slope_n = int(P['emaSlopeN'])
    e200_prev = ema200[-slope_n] if len(ema200) >= slope_n + 1 else ema200[0]

    return {
        'f':        ema5[-1],   'f1':       ema5[-2],
        's':        ema13[-1],  's1':       ema13[-2],
        'tr':       ema50[-1],
        'e200':     ema200[-1], 'e200_prev': e200_prev,
        'e50':      ema50[-1],
        'r':        rsi[-1],
        'a_1m':     atr_1m[-1],
        'b_1m':     bias1m[-1],
        'b_60m':    bias60[-1],
        'r_mtf':    rng[-1],
        'dx':       adx[-1],
        'price':    c[-1],
        'lo':       l[-1],
    }

def check_gates(ind: dict) -> str | None:
    """Return blocking reason string or None if clear."""
    if ind['e200'] < ind['e200_prev']:
        return 'slope_blocked'
    if ind['price'] < ind['e200']:
        return 'below_e200'
    if ind['e50'] < ind['e200']:
        return 'no_golden'
    if ind['dx'] < P['adxMin']:
        return 'adx_low'
    if ind['b_1m'] < P['minBias']:
        return 'bias_low_1m'
    if ind['b_60m'] < P['minBias60m']:
        return 'bias_low_60m'
    if ind['a_1m'] == 0 or ind['r_mtf'] == 0:
        return 'no_atr'
    return None

def check_signal(ind: dict) -> str | None:
    """EMA_CROSS or EMA_PULLBACK — exact conditions from strategy_downtrend.py."""
    f, f1, s, s1 = ind['f'], ind['f1'], ind['s'], ind['s1']
    tr, price, lo = ind['tr'], ind['price'], ind['lo']
    r = ind['r']

    if f <= s:
        return None          # EMA5 not above EMA13
    if price <= tr:
        return None          # price below EMA50

    if f1 <= s1:             # EMA5 just crossed above EMA13
        if r < P['rsiOB']:
            return 'EMA_CROSS'
    else:                    # EMA5 was already above EMA13
        if lo <= f * 1.002 and price > f and P['rsiOS'] < r < 55:
            return 'EMA_PULLBACK'
    return None

def calc_entry_levels(ind: dict) -> dict:
    """Compute stop, target, trail parameters from current indicators."""
    FLOOR   = 0.15
    a_1m    = ind['a_1m']
    b_1m    = ind['b_1m']
    r_mtf   = ind['r_mtf']
    price   = ind['price']

    a_dn_1m  = max(a_1m  * (1 - b_1m) / 2, a_1m  * FLOOR)
    a_up_mtf = max(r_mtf * (1 + b_1m) / 2, r_mtf * FLOOR)

    stop   = price - a_dn_1m * P['atrStop']
    target = price + a_up_mtf * P['atrTp']

    return {
        'entry':          price,
        'stop':           stop,
        'target':         target,
        'a_dn_1m_entry':  a_dn_1m,
    }

# ═══════════════════════════════════════════════════════════════════════════════
# SECTION 4 — Position Manager (state machine: FLAT → IN_POSITION → FLAT)
# ═══════════════════════════════════════════════════════════════════════════════

class PositionManager:
    def __init__(self, balance: float, paper: bool = True):
        self.balance       = balance
        self.paper         = paper
        self.state         = 'FLAT'    # FLAT | IN_POSITION
        self.entry         = 0.0
        self.stop          = 0.0
        self.target        = 0.0
        self.a_dn_entry    = 0.0
        self.bars_held     = 0
        self.btc_qty       = 0.0
        self.entry_order_id   = None
        self.stop_order_id    = None
        self.target_order_id  = None
        self.total_trades  = 0
        self.winning_trades = 0
        self.total_pnl_pct  = 0.0

    # ── called on every bar ───────────────────────────────────────────────────
    def on_bar(self, ind: dict, exchange) -> str:
        action = 'hold'
        if self.state == 'FLAT':
            action = self._check_entry(ind, exchange)
        elif self.state == 'IN_POSITION':
            action = self._manage_position(ind, exchange)
        return action

    def _check_entry(self, ind: dict, exchange) -> str:
        gate = check_gates(ind)
        if gate:
            return f'gate:{gate}'

        sig = check_signal(ind)
        if not sig:
            return 'no_signal'

        levels = calc_entry_levels(ind)
        price  = ind['price']

        # size: use 99% of balance, but cap to keep notional ≥ MIN_NOTIONAL
        usdt_to_spend = self.balance * 0.99
        if usdt_to_spend < MIN_NOTIONAL:
            log.warning(f'Balance ${self.balance:.2f} below min notional — skipping')
            return 'below_min'

        qty = usdt_to_spend / price

        log.info(
            f'SIGNAL {sig} | price={price:.2f} '
            f'stop={levels["stop"]:.2f} target={levels["target"]:.2f} '
            f'qty={qty:.6f} BTC'
        )

        if self.paper:
            self._paper_enter(price, qty, levels, sig)
        else:
            self._live_enter(price, qty, levels, sig, exchange)

        return f'entry:{sig}'

    def _manage_position(self, ind: dict, exchange) -> str:
        price = ind['price']
        self.bars_held += 1

        # ── trail stop update ────────────────────────────────────────────────
        risk_unit = abs(self.entry - self.stop)
        if risk_unit > 0:
            pnl_r = (price - self.entry) / risk_unit
            if pnl_r >= P['trailActivate']:
                trail = price - self.a_dn_entry * P['trailAtr']
                if trail > self.stop:
                    old_stop = self.stop
                    self.stop = trail
                    log.debug(f'Trail stop raised {old_stop:.2f} → {self.stop:.2f}')
                    if not self.paper:
                        self._update_stop_order(exchange)

        # ── exit conditions ───────────────────────────────────────────────────
        reason = None
        exit_price = price

        if price <= self.stop:
            reason = 'stop'
            exit_price = self.stop   # limit stop (maker)
        elif price >= self.target:
            reason = 'target'
            exit_price = self.target
        elif self.bars_held >= P['maxHoldBars']:
            reason = 'timeout'

        if reason:
            # Safety: never sell at market unless profit > taker fee
            if reason == 'timeout':
                pnl_pct = (price - self.entry) / self.entry
                if pnl_pct <= TAKER_FEE:
                    # post a limit sell at a price that covers taker fee
                    min_exit = self.entry * (1 + TAKER_FEE + 0.0001)
                    if price < min_exit:
                        log.debug(
                            f'Timeout exit deferred — price {price:.2f} < '
                            f'min_exit {min_exit:.2f} (taker fee protection)'
                        )
                        return 'hold:fee_protect'
                    exit_price = price

            self._close_position(exit_price, reason, exchange)
            return f'exit:{reason}'

        return 'hold'

    # ── paper fills ───────────────────────────────────────────────────────────
    def _paper_enter(self, price: float, qty: float, levels: dict, sig: str):
        cost = price * qty
        self.balance       -= cost
        self.btc_qty        = qty
        self.entry          = levels['entry']
        self.stop           = levels['stop']
        self.target         = levels['target']
        self.a_dn_entry     = levels['a_dn_1m_entry']
        self.bars_held      = 0
        self.state          = 'IN_POSITION'
        log.info(
            f'[PAPER] BUY {qty:.6f} BTC @ {price:.2f} | '
            f'stop={self.stop:.2f} target={self.target:.2f}'
        )

    def _close_position(self, exit_price: float, reason: str, exchange):
        proceeds = self.btc_qty * exit_price
        pnl_pct  = (exit_price - self.entry) / self.entry * 100
        self.balance += proceeds
        self.total_trades  += 1
        self.total_pnl_pct += pnl_pct
        if pnl_pct > 0:
            self.winning_trades += 1
        win_rate = self.winning_trades / self.total_trades * 100
        log.info(
            f'[{"PAPER" if self.paper else "LIVE"}] SELL {self.btc_qty:.6f} BTC '
            f'@ {exit_price:.2f} reason={reason} pnl={pnl_pct:+.3f}% | '
            f'balance=${self.balance:.4f} trades={self.total_trades} '
            f'W%={win_rate:.0f}%'
        )
        self.state   = 'FLAT'
        self.btc_qty = 0.0

    # ── live order helpers (Binance.US LIMIT + STOP_LOSS_LIMIT) ──────────────
    def _live_enter(self, price, qty, levels, sig, exchange):
        try:
            # LIMIT BUY (maker, 0% fee)
            order = exchange.create_order(
                SYMBOL, 'limit', 'buy', qty,
                price,   # post-at-price
                {'timeInForce': 'GTC', 'newOrderRespType': 'FULL'}
            )
            self.entry_order_id = order['id']
            self.btc_qty        = qty
            self.entry          = levels['entry']
            self.stop           = levels['stop']
            self.target         = levels['target']
            self.a_dn_entry     = levels['a_dn_1m_entry']
            self.bars_held      = 0
            self.state          = 'IN_POSITION'
            log.info(f'[LIVE] LIMIT BUY {qty:.6f} @ {price:.2f} id={order["id"]}')

            # STOP_LOSS_LIMIT sell (stop price = stop - 1 tick; limit = stop - 2 ticks)
            tick = 0.01
            stop_trigger = round(self.stop - tick, 2)
            stop_limit   = round(self.stop - 2 * tick, 2)
            sl = exchange.create_order(
                SYMBOL, 'STOP_LOSS_LIMIT', 'sell', qty,
                stop_limit,
                {
                    'stopPrice':    stop_trigger,
                    'timeInForce':  'GTC',
                }
            )
            self.stop_order_id = sl['id']
            log.info(
                f'[LIVE] STOP_LOSS_LIMIT trigger={stop_trigger:.2f} '
                f'limit={stop_limit:.2f} id={sl["id"]}'
            )

            # LIMIT SELL target (maker)
            tgt = exchange.create_order(
                SYMBOL, 'limit', 'sell', qty,
                round(self.target, 2),
                {'timeInForce': 'GTC'}
            )
            self.target_order_id = tgt['id']
            log.info(
                f'[LIVE] LIMIT SELL target={self.target:.2f} id={tgt["id"]}'
            )
        except Exception as e:
            log.error(f'Live entry failed: {e}')

    def _update_stop_order(self, exchange):
        if not self.stop_order_id:
            return
        try:
            exchange.cancel_order(self.stop_order_id, SYMBOL)
        except Exception:
            pass
        tick = 0.01
        stop_trigger = round(self.stop - tick, 2)
        stop_limit   = round(self.stop - 2 * tick, 2)
        try:
            sl = exchange.create_order(
                SYMBOL, 'STOP_LOSS_LIMIT', 'sell', self.btc_qty,
                stop_limit,
                {'stopPrice': stop_trigger, 'timeInForce': 'GTC'}
            )
            self.stop_order_id = sl['id']
            log.debug(f'[LIVE] Updated stop → trigger={stop_trigger:.2f}')
        except Exception as e:
            log.error(f'Stop update failed: {e}')

    def status(self) -> str:
        if self.state == 'FLAT':
            return (
                f'FLAT | balance=${self.balance:.4f} | '
                f'trades={self.total_trades} | '
                f'total_pnl={self.total_pnl_pct:+.3f}%'
            )
        return (
            f'IN_POSITION | entry={self.entry:.2f} '
            f'stop={self.stop:.2f} target={self.target:.2f} '
            f'bars={self.bars_held}'
        )

# ═══════════════════════════════════════════════════════════════════════════════
# SECTION 5 — Bar Buffer & Feed (WebSocket via ccxt.pro, REST fallback for Termux)
# ═══════════════════════════════════════════════════════════════════════════════

class BarBuffer:
    """Rolling buffer of OHLCV bars, closed-bar semantics."""
    def __init__(self, maxbars: int = 300):
        self.maxbars = maxbars
        self.opens   = deque(maxlen=maxbars)
        self.highs   = deque(maxlen=maxbars)
        self.lows    = deque(maxlen=maxbars)
        self.closes  = deque(maxlen=maxbars)
        self.volumes = deque(maxlen=maxbars)
        self._last_ts = None

    def push(self, ts_ms, o, h, l, c, v) -> bool:
        """Return True when a NEW closed bar is added (timestamp advanced)."""
        if self._last_ts is None:
            self._last_ts = ts_ms
            return False
        if ts_ms <= self._last_ts:
            return False
        self._last_ts = ts_ms
        self.opens.append(o)
        self.highs.append(h)
        self.lows.append(l)
        self.closes.append(c)
        self.volumes.append(v)
        return True

    def ready(self) -> bool:
        return len(self.closes) >= WARMUP_BARS

    def arrays(self):
        return (
            np.array(self.highs),
            np.array(self.lows),
            np.array(self.closes),
        )


def _make_exchange(args, use_pro: bool):
    """Build exchange object. use_pro=True → ccxt.pro (WebSocket), False → ccxt (REST)."""
    creds = {}
    if args.live:
        api_key    = os.environ.get('BINANCE_US_API_KEY',    '')
        api_secret = os.environ.get('BINANCE_US_API_SECRET', '')
        if not api_key or not api_secret:
            log.error('Set BINANCE_US_API_KEY and BINANCE_US_API_SECRET for live mode.')
            sys.exit(1)
        creds = {'apiKey': api_key, 'secret': api_secret}

    opts = {**creds, 'options': {'defaultType': 'spot'}}

    if use_pro:
        import ccxt.pro as ccxtpro
        return ccxtpro.binanceus(opts)
    else:
        import ccxt
        return ccxt.binanceus(opts)


def _on_closed_bar(ts_ms, buf, pm, exchange, bar_count_ref: list):
    """Called for each newly closed bar. Returns action string."""
    bar_count_ref[0] += 1
    bar_count = bar_count_ref[0]

    if not buf.ready():
        if bar_count % 50 == 0:
            log.info('Warmup: %d / %d bars', bar_count, WARMUP_BARS)
        return 'warmup'

    highs, lows, closes = buf.arrays()
    ind    = compute_indicators(highs, lows, closes)
    action = pm.on_bar(ind, exchange)

    ts_str = datetime.fromtimestamp(ts_ms / 1000, tz=timezone.utc).strftime('%H:%M')
    log.debug('%s | price=%.2f RSI=%.1f ADX=%.1f bias=%.3f | %s | %s',
              ts_str, ind['price'], ind['r'], ind['dx'], ind['b_1m'],
              action, pm.status())

    if bar_count % 60 == 0:
        log.info('Heartbeat | %s', pm.status())

    return action


async def _run_ws(args, buf, pm):
    """WebSocket feed — preferred when ccxt.pro is available."""
    exchange = _make_exchange(args, use_pro=True)
    bar_count = [0]
    log.info('Feed: WebSocket (ccxt.pro)')
    try:
        while True:
            candles = await exchange.watch_ohlcv(SYMBOL, '1m')
            for ts_ms, o, h, l, c, v in candles:
                if buf.push(ts_ms, o, h, l, c, v):
                    _on_closed_bar(ts_ms, buf, pm, exchange, bar_count)
    finally:
        await exchange.close()


async def _run_rest(args, buf, pm):
    """REST polling fallback — works on Termux/ARM where ccxt.pro build fails."""
    exchange = _make_exchange(args, use_pro=False)
    bar_count = [0]
    log.info('Feed: REST polling every 62s (ccxt fallback — no WebSocket needed)')

    # Pre-fill buffer with history so indicators are warm immediately
    log.info('Fetching %d bars of history to warm indicators…', WARMUP_BARS + 10)
    try:
        history = exchange.fetch_ohlcv(SYMBOL, '1m', limit=WARMUP_BARS + 10)
        for ts_ms, o, h, l, c, v in history[:-1]:   # skip current open bar
            buf.push(ts_ms, o, h, l, c, v)
        log.info('History loaded: %d bars', len(buf.closes))
    except Exception as e:
        log.warning('History pre-fill failed: %s — will warm up live', e)

    try:
        while True:
            # Sleep until 2 seconds after the next minute boundary
            now      = time.time()
            next_min = (int(now / 60) + 1) * 60 + 2
            wait     = next_min - now
            log.debug('Sleeping %.1fs until next bar close', wait)
            await asyncio.sleep(wait)

            try:
                candles = exchange.fetch_ohlcv(SYMBOL, '1m', limit=5)
            except Exception as e:
                log.error('fetch_ohlcv error: %s — retrying in 10s', e)
                await asyncio.sleep(10)
                continue

            # candles[-1] is the current open bar; candles[-2] is the just-closed bar
            for ts_ms, o, h, l, c, v in candles[:-1]:
                if buf.push(ts_ms, o, h, l, c, v):
                    _on_closed_bar(ts_ms, buf, pm, exchange, bar_count)

    except KeyboardInterrupt:
        pass
    finally:
        log.info('Final | %s', pm.status())


async def run_feed(args):
    """Entry point: try WebSocket, fall back to REST on import / connection error."""
    paper = not args.live
    buf   = BarBuffer(maxbars=max(300, P['emaMacro'] + 10))
    pm    = PositionManager(balance=args.balance, paper=paper)

    log.info('TrendRider v7 DOWNTREND | mode=%s | symbol=%s | balance=$%.2f',
             'PAPER' if paper else 'LIVE', SYMBOL, args.balance)

    # Try WebSocket first; fall back to REST if ccxt.pro is unavailable
    try:
        import ccxt.pro  # noqa: F401 — just testing importability
        await _run_ws(args, buf, pm)
    except (ImportError, ModuleNotFoundError):
        log.warning('ccxt.pro not available — switching to REST polling (Termux-safe)')
        await _run_rest(args, buf, pm)
    except Exception as e:
        log.error('WebSocket feed failed (%s) — switching to REST polling', e)
        await _run_rest(args, buf, pm)

# ═══════════════════════════════════════════════════════════════════════════════
# SECTION 6 — CLI Entry Point
# ═══════════════════════════════════════════════════════════════════════════════

def parse_args():
    ap = argparse.ArgumentParser(
        description='TrendRider v7 DOWNTREND live trader',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  python live_trader.py                      # paper trade, $100
  python live_trader.py --balance 250        # paper trade, $250
  python live_trader.py --live               # real orders (needs env vars)

Environment variables (live mode only):
  BINANCE_US_API_KEY
  BINANCE_US_API_SECRET
""",
    )
    ap.add_argument('--live',     action='store_true',  help='Enable real orders (default: paper)')
    ap.add_argument('--balance',  type=float, default=100.0, help='Starting USDT balance (default: 100)')
    ap.add_argument('--symbol',   type=str,   default=SYMBOL,  help=f'Symbol (default: {SYMBOL})')
    ap.add_argument('--debug',    action='store_true',  help='Verbose debug logging')
    return ap.parse_args()


def main():
    args = parse_args()
    if args.debug:
        logging.getLogger().setLevel(logging.DEBUG)
    if args.symbol != SYMBOL:
        globals()['SYMBOL'] = args.symbol
    asyncio.run(run_feed(args))


if __name__ == '__main__':
    main()
