#!/usr/bin/env python3
"""
physics_live.py — Live wave equation signal engine.

Connects directly to Binance.US WebSocket:
  btcusdc@kline_1m       — 1m bars; processes on bar CLOSE (x=True)
  btcusdc@depth20@100ms  — L20 order book every 100ms

On each closed bar:
  • Runs all physics + microstructure signals with real order book depth
  • Logs the waveform state (KdV, water hammer, Reynolds, dark pool, OBI, CVD)
  • Tracks paper trades (long-only, stop/target/timeout)
  • Writes live stats to physics_results.json every minute
  • Pulls parameter changes from git every 30s (hot-reload)

Run on Termux:
  cd ~/TRENDRIDER
  pip install aiohttp  (if not installed)
  python physics_live.py
"""

import asyncio
import json
import logging
import os
import re
import signal
import subprocess
import sys
import threading
import time
from collections import deque
from datetime import datetime, timezone
from pathlib import Path

import numpy as np

try:
    import aiohttp
except ImportError:
    print("ERROR: pip install aiohttp")
    sys.exit(1)

REPO        = Path(__file__).resolve().parent
CODE_BRANCH = "claude/hft-mean-reversion-strategy-pJ4YU"
DATA_BRANCH = "data/live"
RESULTS_F   = REPO / "physics_live_results.json"   # separate from optimizer
TRIGGER_F   = REPO / "physics_live_trigger.json"
LOG_F       = REPO / "physics_live.log"

WS_URL = ("wss://stream.binance.us:9443/stream"
          "?streams=btcusdc@kline_1m/btcusdc@depth20@100ms")

FETCH_EVERY_S = 30   # pull param changes from git

G='\033[92m'; R='\033[91m'; Y='\033[93m'; C='\033[96m'; B='\033[1m'; Z='\033[0m'

ENV = {**os.environ, "GIT_TERMINAL_PROMPT": "0"}

# Serialize all git operations — _push_results and _fetch_params run in
# separate thread executors; concurrent git calls corrupt repo state and
# cause spurious physics_live.py-changed detections → restart loops.
_GIT_LOCK = threading.Lock()


def ts_str():
    return datetime.now().strftime('%H:%M:%S')


def log(msg, col=Z):
    line = f"{col}[live {ts_str()}] {msg}{Z}"
    print(line, flush=True)
    try:
        with open(LOG_F, 'a') as f:
            f.write(re.sub(r'\033\[[0-9;]*m', '', line) + '\n')
    except Exception:
        pass


def git(*args):
    return subprocess.run(["git"] + list(args), cwd=REPO,
                          capture_output=True, text=True, env=ENV)


# ── Physics hot-reload ────────────────────────────────────────────────────────

def flush_physics():
    for key in list(sys.modules.keys()):
        if key.startswith('physics'):
            del sys.modules[key]


def load_physics():
    flush_physics()
    from physics.signals  import HydraulicAccumulator
    from physics.position import PositionManager
    from physics          import config as CF
    return CF, HydraulicAccumulator, PositionManager


# ── ATR ───────────────────────────────────────────────────────────────────────

def _atr(highs, lows, closes, period=14):
    n = len(closes)
    out = np.zeros(n)
    if n < period + 2:
        return out
    trs = np.array([
        max(highs[i] - lows[i],
            abs(highs[i]  - closes[i-1]),
            abs(lows[i]   - closes[i-1]))
        for i in range(1, n)
    ])
    out[period] = float(np.mean(trs[:period]))
    for i in range(period, len(trs)):
        out[i+1] = (out[i] * (period-1) + trs[i]) / period
    return out


# ── Bootstrap: seed 1m window + HTF bars ─────────────────────────────────────

def _parse_bars(raw, interval: str) -> list:
    from physics import data as D
    data = D.parse_klines(raw)
    bars = []
    for i in range(len(data['closes'])):
        bars.append({
            'ts':        int(data['timestamps'][i]),
            'open':      float(data['opens'][i]),
            'high':      float(data['highs'][i]),
            'low':       float(data['lows'][i]),
            'close':     float(data['closes'][i]),
            'volume':    float(data['volumes'][i]),
            'taker_buy': float(data.get('taker_buy', [0]*len(data['closes']))[i]),
            'interval':  interval,
        })
    return bars


def bootstrap_rest(symbol: str, n: int = 300) -> list:
    """Return list of 1m bar dicts from REST (for ATR/signal warmup)."""
    log(f"Fetching {n} 1m bars from Binance REST for warmup...", C)
    flush_physics()
    try:
        from physics import data as D, config as CF
        raw  = D.fetch_klines_bulk(symbol, CF.INTERVAL, n)
        bars = _parse_bars(raw, '1m')
        for b in bars:
            b['live'] = False
        log(f"  {len(bars)} 1m REST bars loaded", G)
        return bars
    except Exception as e:
        log(f"REST bootstrap error: {e}", R)
        return []


def bootstrap_htf(symbol: str) -> dict:
    """Fetch 5m, 15m, 1h, and 4h bars for immediate HTF regime context."""
    from physics import data as D, config as CF
    htf = {'5m': [], '15m': [], '1h': [], '4h': []}
    for interval, n_bars, key in [
        ('5m',  CF.HTF_5M_BARS,  '5m'),
        ('15m', CF.HTF_15M_BARS, '15m'),
        ('1h',  CF.HTF_1H_BARS,  '1h'),
        ('4h',  CF.HTF_4H_BARS,  '4h'),
    ]:
        try:
            raw  = D.fetch_klines_bulk(symbol, interval, n_bars)
            bars = _parse_bars(raw, interval)
            htf[key] = bars
            log(f"  HTF {interval}: {len(bars)} bars loaded", G)
        except Exception as e:
            log(f"  HTF {interval} bootstrap error: {e}", Y)
    return htf


# ── Paper trade tracker ───────────────────────────────────────────────────────

class PaperTrader:
    """
    Tracks live paper trades using real-time price data.
    One position at a time, long-only (flat cash, no margin).
    """

    def __init__(self, CF, PositionManager):
        self._CF          = CF
        self._pm          = PositionManager(CF.INITIAL_BAL)
        self._open        = None
        self._trades      = []
        self._bars        = 0
        self._start       = time.time()
        self._last_bids_n = 0    # depth diagnostic
        self._last_cav    = {}   # cavitation breakdown
        self._last_htf    = {}   # HTF regime context

    def reload_params(self, CF):
        self._CF = CF

    def scan_intrabar(self, partial_bar: dict, bids, asks, accum):
        """
        Intrabar update — do NOT call fusion.run() here.
        The HydraulicAccumulator is stateful; calling fusion dozens of times
        per minute saturates it and freezes the score at a fixed value.
        Intrabar monitoring is handled by _handle_depth() (peak OB walls,
        dark pool events) which is stateless and safe to call on every push.
        """
        pass

    def tick_price(self, price: float) -> bool:
        """
        Called on EVERY intrabar kline update (x=False).
        Checks stop/target on the live price — don't wait 60s for bar close.
        Returns True if a trade was closed so the caller can write stats.
        """
        if self._open is None:
            return False
        t   = self._open
        cur = price
        t['mae'] = max(t['mae'], float(t['entry'] - cur))
        t['mfe'] = max(t['mfe'], float(cur - t['entry']))

        reason = exit_px = None
        if cur <= t['stop']:     reason, exit_px = 'stop',   t['stop']
        elif cur >= t['target']: reason, exit_px = 'target', t['target']

        if reason:
            pnl = (exit_px - t['entry']) / t['entry'] * 100.0
            t.update(pnl_pct=pnl, exit_price=exit_px,
                     exit_reason=reason, exit_bar=self._bars)
            self._pm.on_close(pnl, t['mae'], t['tier'], t['size_usd'])
            self._trades.append(t)
            self._open = None
            col = G if pnl > 0 else R
            log(f"  [TICK {reason}] pnl={pnl:+.3f}%  tier={t['tier']}", col)
            return True
        return False

    def on_bar(self, bar: dict, bids, asks, accum, htf_ctx: dict = None,
               bar_ask_cleared: bool = False, bar_obi_bull: float = 0.0):
        """
        Process one closed 1m bar.
        Stop/target already handled by tick_price() intrabar.
        htf_ctx: pre-computed regime from _update_live_htf() — reflects the
                 bar's live high/low, updated on every kline packet not just
                 bar close.
        """
        from physics import fusion, config as CF

        self._bars += 1
        cur = bar['close']

        # ── Manage open trade (timeout only — stop/target caught by tick_price) ──
        if self._open is not None:
            t = self._open
            t['mae'] = max(t['mae'], float(t['entry'] - cur))
            t['mfe'] = max(t['mfe'], float(cur - t['entry']))

            if self._bars - t['bar_idx'] >= self._CF.MAX_HOLD_BARS:
                pnl = (cur - t['entry']) / t['entry'] * 100.0
                t.update(pnl_pct=pnl, exit_price=cur,
                         exit_reason='timeout', exit_bar=self._bars)
                self._pm.on_close(pnl, t['mae'], t['tier'], t['size_usd'])
                self._trades.append(t)
                self._open = None
                col = G if pnl > 0 else R
                log(f"  [timeout] pnl={pnl:+.3f}%  "
                    f"held={self._CF.MAX_HOLD_BARS}bars  tier={t['tier']}", col)

        if self._open is not None:
            return None   # one position at a time

        # ── Signal engine (ALL physics + microstructure) ──────────────────
        w = self._window_arrays()
        n_bars = len(w['closes'])
        if n_bars < self._CF.WARMUP_BARS:
            log(f"  warmup {n_bars}/{self._CF.WARMUP_BARS} bars", C)
            return None

        sl = slice(max(0, n_bars - 600), n_bars)
        p  = w['prices'][sl];   o = w['opens'][sl]
        c  = w['closes'][sl];   v = w['volumes'][sl]
        tb = w['taker_buy'][sl]

        atr_arr = _atr(w['highs'], w['lows'], w['closes'], self._CF.MAE_ATR_PERIOD)
        atr = float(atr_arr[-1]) if atr_arr[-1] > 0 else cur * 0.002

        sig = fusion.run(p, o, c, v, tb, accum, bids=bids, asks=asks)

        self._last_bids_n = len(bids) if bids else 0
        self._last_cav    = sig.get('cavitation', {})
        self._last_score  = float(sig.get('score', 0))
        self._last_snr    = float(sig.get('snr', 0))
        self._last_dir    = int(sig.get('direction', 0))
        self._last_tier   = int(sig.get('tier', 4))

        # ── HTF regime context (pre-computed every kline packet — always live) ─
        if htf_ctx:
            self._last_htf = htf_ctx
            log(f"  HTF 1m={htf_ctx.get('trend_1m','?')} "
                f"5m={htf_ctx.get('trend_5m','?')} "
                f"15m={htf_ctx.get('trend_15m','?')} "
                f"1h={htf_ctx['trend_1h']} 4h={htf_ctx['trend_4h']}  "
                f"bias={htf_ctx['bias']:+.2f}  "
                f"sup={htf_ctx['at_support']}  rev={htf_ctx['reversal_setup']}  "
                f"fk={htf_ctx['falling_knife']}", C)

        log(f"  score={self._last_score:+.4f}  dir={self._last_dir:+d}  "
            f"tier={self._last_tier}  snr={self._last_snr:.2f}  "
            f"threshold={self._CF.LONG_THRESHOLD}", C)

        # ── Reversal override: enter long at HTF support even if score < threshold
        score_abs = abs(self._last_score)
        reversal_ok = (
            htf_ctx is not None and
            htf_ctx['reversal_setup'] and
            score_abs >= CF.HTF_REVERSAL_MIN_SCORE
        )

        # Pre-flip entry: physics saturated bearish at support + depth confirms
        # accumulation (asks being pulled / bid OBI loading) before dir flips.
        # The depth signals lead the physics wave — entering here catches the
        # bottom of the upswing instead of the middle.
        preflip_ok = (
            htf_ctx is not None and
            htf_ctx['reversal_setup'] and
            self._last_score <= -CF.LONG_THRESHOLD and
            (bar_ask_cleared or bar_obi_bull >= 0.40)
        )

        if not reversal_ok and not preflip_ok:
            if sig['direction'] != 1 or sig['tier'] >= 4:
                return sig
        if sig['snr'] < self._CF.SNR_MIN:
            return sig

        # ── HTF entry gate ────────────────────────────────────────────────────
        # Depth signals (intra-bar) lead the physics wave and fusion-level OBI.
        # If either the pre-flip or reversal window is active AND depth confirms
        # accumulation, bypass the fusion WH/CVD/OBI gate entirely.
        depth_confirmed = bar_ask_cleared or bar_obi_bull >= 0.40
        if depth_confirmed and (preflip_ok or reversal_ok):
            allowed = True
            reason  = (f"depth confirm: "
                       f"ask_cleared={bar_ask_cleared}  obi_bull={bar_obi_bull:.2f}")
            log(f"  [PRE-FLIP] {reason}", G)
        else:
            from physics import htf as HTF
            allowed, reason = HTF.allows_entry(sig, htf_ctx)
        if not allowed:
            log(f"  [HTF block] {reason}", Y)
            return sig

        # Pre-flip entries use tier=2: depth-confirmed but physics wave hasn't
        # turned yet — size conservatively until the wave confirms.
        entry_tier = 2 if (preflip_ok and not reversal_ok) else sig['tier']

        entry  = cur
        stop   = self._pm.entry_stop(entry, +1, atr, entry_tier)
        target = self._pm.entry_target(entry, stop, +1, entry_tier, atr)
        size   = self._pm.size(entry_tier, sig['confidence'])

        self._open = {
            'bar_idx':    self._bars,
            'ts':         bar['ts'],
            'direction':  +1,
            'entry':      entry,
            'stop':       stop,
            'target':     target,
            'tier':       entry_tier,
            'size_usd':   size,
            'confidence': sig['confidence'],
            'mae':        0.0,
            'mfe':        0.0,
        }
        return sig

    # window stored by LiveEngine and passed in — keep a ref
    _w = None

    def set_window(self, window):
        self._w = window

    def _window_arrays(self):
        bars = list(self._w)
        return {
            'timestamps': np.array([b['ts']        for b in bars], dtype=np.int64),
            'opens':      np.array([b['open']       for b in bars]),
            'highs':      np.array([b['high']       for b in bars]),
            'lows':       np.array([b['low']        for b in bars]),
            'closes':     np.array([b['close']      for b in bars]),
            'prices':     np.array([b['close']      for b in bars]),
            'volumes':    np.array([b['volume']     for b in bars]),
            'taker_buy':  np.array([b['taker_buy']  for b in bars]),
        }

    # Last fusion scores — updated every bar for remote diagnostics
    _last_score: float = 0.0
    _last_snr:   float = 0.0
    _last_dir:   int   = 0
    _last_tier:  int   = 4

    def stats(self):
        CF = self._CF
        n      = len(self._trades)
        wins   = [t for t in self._trades if t['pnl_pct'] > 0]
        hr     = max((time.time() - self._start) / 3600, 1/60)
        tph    = n / hr
        win_rt = len(wins) / n * 100 if n else 0.0
        pnls   = [t['pnl_pct'] for t in self._trades]

        # Properly-sized equity curve
        bal    = CF.INITIAL_BAL
        equity = [bal]
        for t in self._trades:
            sized = t['pnl_pct'] / 100.0 * (t['size_usd'] / max(bal, 1.0))
            bal  *= (1.0 + sized)
            equity.append(bal)
        eq = np.array(equity)

        # Per-trade Sharpe scaled to annual
        if len(pnls) >= 3:
            r    = np.array(pnls)
            std  = float(np.std(r))
            tpy  = n / max(self._bars, 1) * (252 * 1440)
            sh   = float(np.mean(r) / (std + 1e-10) * np.sqrt(max(tpy, 1)))
        else:
            sh = 0.0

        max_dd = 0.0
        if len(eq) >= 2:
            peak   = np.maximum.accumulate(eq)
            max_dd = float(np.min((eq - peak) / (peak + 1e-10) * 100))

        d = {
            'n_trades':      n,
            'trades_per_hr': round(tph, 2),
            'win_rate':      round(win_rt, 2),
            'sharpe':        round(sh, 4),
            'max_dd':        round(max_dd, 4),
            'avg_pnl':       round(float(np.mean(pnls)), 4) if pnls else 0.0,
            'equity_final':  round(float(eq[-1]), 2),
            'bars_live':     self._bars,
            # Diagnostics — last fusion score so remote monitor can see why
            # signals aren't firing without needing Termux access
            'last_score':    round(self._last_score, 4),
            'last_snr':      round(self._last_snr, 2),
            'last_dir':      self._last_dir,
            'last_tier':     self._last_tier,
            'threshold':     float(CF.LONG_THRESHOLD),
            'snr_min':       float(CF.SNR_MIN),
            # OB / cavitation diagnostics
            'last_bids_n':     self._last_bids_n,
            'last_cav_active': bool(self._last_cav.get('active', False)),
            'last_cav_risk':   round(float(self._last_cav.get('risk', 0)), 4),
            # HTF regime
            'htf_trend_1m':   self._last_htf.get('trend_1m',  'none'),
            'htf_trend_5m':   self._last_htf.get('trend_5m',  'none'),
            'htf_trend_15m':  self._last_htf.get('trend_15m', 'none'),
            'htf_trend_1h':   self._last_htf.get('trend_1h',  'none'),
            'htf_trend_4h':   self._last_htf.get('trend_4h',  'none'),
            'htf_bias':       round(float(self._last_htf.get('bias', 0)), 3),
            'htf_at_sup':     bool(self._last_htf.get('at_support', False)),
            'htf_reversal':   bool(self._last_htf.get('reversal_setup', False)),
            'htf_fk':         bool(self._last_htf.get('falling_knife', False)),
        }
        for tier in [1, 2, 3]:
            tt = [t for t in self._trades if t['tier'] == tier]
            if tt:
                tw = [x for x in tt if x['pnl_pct'] > 0]
                d[f't{tier}_n']        = len(tt)
                d[f't{tier}_win_rate'] = round(len(tw)/len(tt)*100, 1)
        return d


# ── Main async engine ─────────────────────────────────────────────────────────

class LiveEngine:
    """
    Connects directly to Binance.US WebSocket.
    Processes each 1m bar close with real-time L20 order book.
    """

    def __init__(self):
        CF, HydAccum, PM = load_physics()
        self._CF     = CF
        self._accum  = HydAccum()
        self._trader = PaperTrader(CF, PM)
        self._window = deque(maxlen=700)
        self._trader.set_window(self._window)

        # Current order book — updated on every WebSocket depth push (variable rate)
        self._bids: list = []
        self._asks: list = []

        # Peak wall tracking across all pushes since last bar close.
        # Captures dark pool walls that post and absorb mid-bar before close.
        self._peak_bid_wall: float = 0.0
        self._peak_ask_wall: float = 0.0
        self._peak_bids: list = []
        self._peak_asks: list = []

        # HTF bars — seeded via REST, updated from live stream on every packet.
        # _htf_*: completed candles per timeframe.
        # _live_*: running partial candle — updated every kline push so
        # htf.regime() always reflects the current bar's live high/low.
        # 1m bars come from self._window (closed bars only — no separate live).
        self._htf_5m:   list  = []
        self._htf_15m:  list  = []
        self._htf_1h:   list  = []
        self._htf_4h:   list  = []
        self._live_5m:  dict  = {}
        self._live_15m: dict  = {}
        self._live_1h:  dict  = {}
        self._live_4h:  dict  = {}
        self._htf_ctx:  dict  = {}   # latest regime — recomputed every kline packet

        self._last_fetch  = 0.0
        self._running     = True
        self._htf_mod     = None   # cached after each reload
        self._last_dark_side: str   = ''
        self._last_dark_obi:  float = 0.0

        # Intra-bar depth signal flags — leading indicators for pre-flip entries.
        # Set during the bar by _handle_depth(); passed to on_bar(); reset at bar close.
        self._bar_ask_cleared: bool  = False  # [CLEAR ASK] fired this bar
        self._bar_obi_bull:    float = 0.0    # peak bullish depth OBI this bar

        # Absorption tracking — filled vs cancelled volume per bar.
        # Filled: qty decreases at level currently AT the spread (taker hit it).
        # Pulled: qty decreases at level AWAY from spread (maker cancelled).
        self._bid_filled:  float = 0.0   # BTC absorbed on bid side (taker sells)
        self._ask_filled:  float = 0.0   # BTC absorbed on ask side (taker buys)
        self._bid_pulled:  float = 0.0   # BTC cancelled on bid side (spoofed)
        self._ask_pulled:  float = 0.0   # BTC cancelled on ask side (spoofed)

    def _reload(self):
        """Hot-reload params from updated config.py."""
        flush_physics()
        from physics import config as CF
        from physics import htf as HTF   # pre-warm so next packet doesn't fail
        self._CF      = CF
        self._htf_mod = HTF
        self._trader.reload_params(CF)
        log(f"Params reloaded: threshold={CF.LONG_THRESHOLD}  "
            f"SNR={CF.SNR_MIN}  tier3_target={CF.TIER3_TARGET_ATR}×ATR", C)

    def _update_live_htf(self, k: dict):
        """
        Update 5m / 15m / 1h / 4h partial bars from every kline packet.
        Called on EVERY push so htf.regime() always reflects the current
        bar's live high/low/close, not a 60-second snapshot.

        1m structure comes directly from self._window (closed 1m bars).
        """
        ts_ms    = int(k['t'])
        m5_slot  = ts_ms // 300_000
        m10_slot = ts_ms // 900_000
        h1_slot  = ts_ms // 3_600_000
        h4_slot  = ts_ms // 14_400_000
        cur      = float(k['c'])

        for slot, live_attr, hist_attr, maxbars in [
            (m5_slot,  '_live_5m',  '_htf_5m',  600),
            (m10_slot, '_live_15m', '_htf_15m', 300),
            (h1_slot,  '_live_1h',  '_htf_1h',  100),
            (h4_slot,  '_live_4h',  '_htf_4h',  40),
        ]:
            live = getattr(self, live_attr)
            hist = getattr(self, hist_attr)

            if not live or live.get('slot') != slot:
                if live:
                    hist.append({
                        'ts':    live['ts'],
                        'open':  live['open'],
                        'high':  live['high'],
                        'low':   live['low'],
                        'close': live['close'],
                    })
                    if len(hist) > maxbars * 2:
                        setattr(self, hist_attr, hist[-maxbars:])
                setattr(self, live_attr, {
                    'slot':  slot,
                    'ts':    ts_ms,
                    'open':  float(k['o']),
                    'high':  float(k['h']),
                    'low':   float(k['l']),
                    'close': cur,
                })
            else:
                live['high']  = max(live['high'],  float(k['h']))
                live['low']   = min(live['low'],   float(k['l']))
                live['close'] = cur

        # All TFs = historical + current partial bar
        bars_5m  = list(self._htf_5m)  + ([self._live_5m]  if self._live_5m  else [])
        bars_15m = list(self._htf_15m) + ([self._live_15m] if self._live_15m else [])
        bars_1h  = list(self._htf_1h)  + ([self._live_1h]  if self._live_1h  else [])
        bars_4h  = list(self._htf_4h)  + ([self._live_4h]  if self._live_4h  else [])
        # 1m: use closed bars from window (REST-seeded + live closed bars)
        bars_1m  = list(self._window)

        if any(len(b) >= 4 for b in [bars_1h, bars_4h, bars_5m, bars_15m, bars_1m]):
            if self._htf_mod is None:
                from physics import htf as HTF
                self._htf_mod = HTF
            self._htf_ctx = self._htf_mod.regime(
                cur, bars_1h, bars_4h,
                bars_1m=bars_1m, bars_5m=bars_5m, bars_15m=bars_15m,
            )

    def _handle_kline(self, k: dict):
        is_closed = k.get('x', False)
        cur_price = float(k['c'])

        # Every packet: stop/target check on live price
        trade_closed = self._trader.tick_price(cur_price)
        if trade_closed:
            self._write_stats()

        # Every packet: update live HTF partial bars + recompute regime.
        # This means htf.regime() always reflects the bar's current high/low,
        # not a 60-second-old snapshot.
        self._update_live_htf(k)

        bar = {
            'ts':        int(k['t']),
            'open':      float(k['o']),
            'high':      float(k['h']),
            'low':       float(k['l']),
            'close':     cur_price,
            'volume':    float(k['v']),
            'taker_buy': float(k['V']),
            'live':      True,
        }

        bids = list(self._peak_bids or self._bids)
        asks = list(self._peak_asks or self._asks)

        if not is_closed:
            self._trader.scan_intrabar(bar, bids or None, asks or None, self._accum)
            return

        # Bar closed — commit and run full signal engine (fusion + accumulator)
        self._window.append(bar)

        sig = self._trader.on_bar(
            bar, bids or None, asks or None, self._accum,
            htf_ctx=self._htf_ctx,
            bar_ask_cleared=self._bar_ask_cleared,
            bar_obi_bull=self._bar_obi_bull,
        )
        if sig:
            self._log_waveforms(bar, sig, bids)

        # Log bar absorption summary and reset counters
        if (self._bid_filled + self._ask_filled +
                self._bid_pulled + self._ask_pulled) > 0.001:
            net = self._bid_filled - self._ask_filled
            log(f"  [ABSORB] bid_fill={self._bid_filled:.4f}  "
                f"ask_fill={self._ask_filled:.4f}  "
                f"bid_pull={self._bid_pulled:.4f}  "
                f"ask_pull={self._ask_pulled:.4f}  "
                f"net={net:+.4f}BTC", Y)
        self._bid_filled = self._ask_filled = 0.0
        self._bid_pulled = self._ask_pulled = 0.0
        self._bar_ask_cleared = False
        self._bar_obi_bull    = 0.0

        # Reset peak OB tracking for the next bar
        self._peak_bid_wall = 0.0
        self._peak_ask_wall = 0.0
        self._peak_bids     = []
        self._peak_asks     = []
        self._last_dark_side = ''
        self._last_dark_obi  = 0.0

        self._write_stats()

    def _handle_depth(self, data: dict):
        new_bids = [(float(p), float(q)) for p, q in data.get('bids', [])]
        new_asks = [(float(p), float(q)) for p, q in data.get('asks', [])]

        # Absorption measurement before overwriting current snapshot.
        # Structural distinction — filled vs pulled is about spread movement:
        #
        #   FILLED: the spread moved THROUGH the level — the level is now
        #           behind the new best bid/ask. Price had to cross it to get
        #           where it is now. Takers consumed it.
        #
        #   PULLED: the level decreased/vanished but the spread did NOT move
        #           through it — price never reached it. Maker cancelled.
        #
        # Ask side rule: price < new_best_ask → spread stepped past it → FILLED
        #                price >= new_best_ask → spread didn't reach it → PULLED
        # Bid side rule: price > new_best_bid → spread stepped past it → FILLED
        #                price <= new_best_bid → spread didn't reach it → PULLED
        if self._bids and self._asks and new_bids and new_asks:
            new_best_ask = new_asks[0][0]
            new_best_bid = new_bids[0][0]

            # Ask side
            prev_ask = {p: q for p, q in self._asks}
            new_ask_map = {p: q for p, q in new_asks}
            for price, prev_qty in prev_ask.items():
                new_qty = new_ask_map.get(price, 0.0)
                if prev_qty > new_qty:
                    delta = prev_qty - new_qty
                    if price < new_best_ask:
                        self._ask_filled += delta
                    else:
                        self._ask_pulled += delta
                        # Large instantaneous pull on ask = resistance cleared.
                        # Someone yanked liquidity before price reached it —
                        # the path up is being deliberately opened. Bullish.
                        pull_pct = delta / prev_qty
                        if pull_pct >= 0.50 and delta >= 0.05:
                            log(f"  [CLEAR ASK] {delta:.4f}BTC pulled "
                                f"({pull_pct:.0%} of level @{price:.2f}) "
                                f"→ path opened UPSIDE", G)
                            self._bar_ask_cleared = True

            # Bid side
            prev_bid = {p: q for p, q in self._bids}
            new_bid_map = {p: q for p, q in new_bids}
            for price, prev_qty in prev_bid.items():
                new_qty = new_bid_map.get(price, 0.0)
                if prev_qty > new_qty:
                    delta = prev_qty - new_qty
                    if price > new_best_bid:
                        self._bid_filled += delta
                    else:
                        self._bid_pulled += delta
                        # Large pull on bid = support removed before price got there.
                        # Buyers stepped aside intentionally — path down cleared. Bearish.
                        pull_pct = delta / prev_qty
                        if pull_pct >= 0.50 and delta >= 0.05:
                            log(f"  [CLEAR BID] {delta:.4f}BTC pulled "
                                f"({pull_pct:.0%} of level @{price:.2f}) "
                                f"→ path opened DOWNSIDE", R)

        self._bids = new_bids
        self._asks = new_asks

        if not new_bids or not new_asks:
            return

        # Peak wall tracking
        top_bid = new_bids[0][1]
        top_ask = new_asks[0][1]
        if top_bid > self._peak_bid_wall:
            self._peak_bid_wall = top_bid
            self._peak_bids     = new_bids
        if top_ask > self._peak_ask_wall:
            self._peak_ask_wall = top_ask
            self._peak_asks     = new_asks

        # OBI imbalance — log on change with filled/pulled context
        bid_vol5 = sum(q for _, q in new_bids[:5])
        ask_vol5 = sum(q for _, q in new_asks[:5])
        total5   = bid_vol5 + ask_vol5
        if total5 > 0:
            obi = (bid_vol5 - ask_vol5) / total5
            if obi > 0:
                self._bar_obi_bull = max(self._bar_obi_bull, obi)
            if abs(obi) > 0.40:
                side = 'BID' if obi > 0 else 'ASK'
                if side != self._last_dark_side or abs(obi - self._last_dark_obi) > 0.05:
                    self._last_dark_side = side
                    self._last_dark_obi  = obi
                    bf = self._bid_filled; af = self._ask_filled
                    bp = self._bid_pulled; ap = self._ask_pulled
                    total = bf + af + bp + ap
                    ratio = (bf + af) / total if total > 0 else 0.0
                    # ratio → 1.0 = all real absorption, → 0.0 = all fake walls
                    log(f"  [DARK] {side} OBI={obi:+.3f}  "
                        f"bid5={bid_vol5:.3f}  ask5={ask_vol5:.3f}  "
                        f"fill={bf+af:.4f}  pull={bp+ap:.4f}  "
                        f"real={ratio:.0%}", Y)
            else:
                self._last_dark_side = ''
                self._last_dark_obi  = 0.0

    def _log_waveforms(self, bar: dict, sig: dict, bids: list):
        """Print the wave equation breakdown for every signal bar."""
        sol = sig['soliton'];      wh = sig['water_hammer']
        acc = sig['accum'];        re = sig['reynolds']
        sh  = sig['shock'];        d  = sig['darcy']
        cav = sig['cavitation'];   mf = sig['mass_flow']
        cvd = sig['cvd'];          obi= sig['obi']

        has_ob = bool(bids)
        ob_str = f"OBI={obi['obi']:+.3f}" if has_ob else "OBI=proxy(no book)"

        log(f"BAR {datetime.fromtimestamp(bar['ts']/1000).strftime('%H:%M')} "
            f"close={bar['close']:.2f}  score={sig['score']:.3f}  "
            f"snr={sig['snr']:.2f}  tier={sig['tier']}", B)
        log(f"  KdV-soliton  detected={sol['detected']}  "
            f"balance={sol.get('balance',0):.3f}  dir={sol.get('direction',0):+d}", C)
        log(f"  WaterHammer  detected={wh['detected']}  "
            f"strength={wh.get('strength',0):.3f}", C)
        log(f"  DarkPool     firing={acc['firing']}  "
            f"pressure={acc.get('pressure',0):.3f}  dir={acc.get('direction',0):+d}", C)
        log(f"  Reynolds     regime={re['regime']}  "
            f"Re={re.get('re',0):.1f}  mult={re.get('multiplier',1):.2f}", C)
        log(f"  Shock        detected={sh['detected']}  "
            f"Mach={sh.get('mach',0):.3f}", C)
        log(f"  Darcy        Q={d['Q']:.3f}  friction={d.get('friction',0):.3f}", C)
        log(f"  Cavitation   active={cav['active']}  "
            f"mult={cav.get('multiplier',1):.2f}", C)
        log(f"  MassFlow     flow={mf.get('flow',0):.3f}", C)
        log(f"  CVD          strong={cvd['strong']}  "
            f"div={cvd.get('divergence',0):.3f}", C)
        log(f"  {ob_str}  physics={sig['physics']:.3f}  "
            f"micro={sig['micro']:.3f}", C)

        if sig['direction'] == 1 and sig['tier'] < 4:
            t = self._trader._open
            if t:
                log(f"  → ENTERED  stop={t['stop']:.2f}  "
                    f"target={t['target']:.2f}", Y)

    def _write_stats(self):
        try:
            trigger_id = json.loads(TRIGGER_F.read_text()).get('id', 'p0')
        except Exception:
            trigger_id = 'p0'

        s = self._trader.stats()
        col = G if s['win_rate'] >= 60 else (Y if s['win_rate'] >= 45 else R)
        log(f"[{trigger_id}] sharpe={s['sharpe']:+.3f} | "
            f"win={s['win_rate']:.1f}% | tph={s['trades_per_hr']:.2f} | "
            f"n={s['n_trades']}", col)

        result = {
            'ts':         datetime.now(timezone.utc).isoformat(),
            'trigger_id': trigger_id,
            'status':     'live',
            'stats':      s,
        }
        content = json.dumps(result, indent=2)
        RESULTS_F.write_text(content)
        # Push in background thread — don't block the async event loop
        asyncio.get_event_loop().run_in_executor(None, self._push_results, content)

    def _push_results(self, content: str):
        """Runs in a thread executor — never blocks the WebSocket event loop."""
        rel = str(RESULTS_F.relative_to(REPO))
        with _GIT_LOCK:
            try:
                git("stash")
                git("fetch", "origin", DATA_BRANCH)
                r = git("checkout", "-B", DATA_BRANCH, f"origin/{DATA_BRANCH}")
                if r.returncode != 0:
                    log(f"  push: checkout data/live failed: {r.stderr[:80]}", R)
                    git("checkout", CODE_BRANCH)
                    git("stash", "pop")
                    RESULTS_F.write_text(content)
                    return
                RESULTS_F.write_text(content)
                git("add", rel)
                git("commit", "--allow-empty", "-m", f"live {ts_str()}")
                pushed = False
                for wait in [0, 2, 4, 8]:
                    time.sleep(wait)
                    r = git("push", "-u", "origin", DATA_BRANCH, "--force")
                    if r.returncode == 0:
                        pushed = True
                        break
                    log(f"  push attempt failed: {r.stderr[:60]}", Y)
                if not pushed:
                    log("  push: all attempts failed — optimizer reads from local file", R)
            except Exception as e:
                log(f"  push error: {e}", R)
            finally:
                git("checkout", CODE_BRANCH)
                git("stash", "pop")
                RESULTS_F.write_text(content)   # restore local copy

    async def _fetch_params(self):
        """Pull latest params from remote (every FETCH_EVERY_S seconds).
        If physics_live.py itself changed, stop cleanly — start_live.sh restarts.
        If only physics/* changed, flush and hot-reload in-process.
        """
        now = time.time()
        if now - self._last_fetch < FETCH_EVERY_S:
            return
        self._last_fetch = now

        def _pull():
            with _GIT_LOCK:
                git("fetch", "origin", CODE_BRANCH, "--quiet")
                r = git("diff", f"HEAD..origin/{CODE_BRANCH}", "--name-only")
                changed = set(r.stdout.strip().split('\n')) if r.stdout.strip() else set()
                git("reset", "--hard", f"origin/{CODE_BRANCH}")
            return changed

        changed = await asyncio.get_event_loop().run_in_executor(None, _pull)

        if 'physics_live.py' in changed:
            log("physics_live.py updated — exiting for full reload (start_live.sh restarts)", Y)
            self.stop()
            return

        self._reload()

    async def run(self):
        backoff = 1
        while self._running:
            try:
                async with aiohttp.ClientSession() as sess:
                    async with sess.ws_connect(WS_URL, heartbeat=20,
                                               receive_timeout=90) as ws:
                        backoff = 1
                        log(f"Connected to {WS_URL}", G)
                        async for msg in ws:
                            if not self._running:
                                break
                            if msg.type == aiohttp.WSMsgType.TEXT:
                                try:
                                    d      = json.loads(msg.data)
                                    stream = d.get('stream', '')
                                    data   = d.get('data', {})
                                    if '@kline' in stream:
                                        self._handle_kline(data.get('k', {}))
                                    elif '@depth' in stream:
                                        self._handle_depth(data)
                                    await self._fetch_params()
                                except Exception as e:
                                    log(f"Parse error: {e}", R)
                            elif msg.type in (aiohttp.WSMsgType.CLOSED,
                                              aiohttp.WSMsgType.ERROR):
                                log(f"WS closed/error — reconnecting", Y)
                                break
            except asyncio.CancelledError:
                break
            except Exception as e:
                if not self._running:
                    break
                log(f"Connection error ({e}) — retry in {backoff}s", R)
                await asyncio.sleep(backoff)
                backoff = min(backoff * 2, 60)

        log("Engine stopped", Y)
        self._write_stats()

    def stop(self):
        self._running = False


# ── Entry point ───────────────────────────────────────────────────────────────

async def _main():
    flush_physics()
    from physics import config as CF

    log(f"Physics Live Engine — {CF.SYMBOL}", B)
    log(f"  Direct WebSocket → Binance.US", G)
    log(f"  Streams: kline_1m (bar close) + depth20@100ms (real OB)", G)
    log(f"  Params: threshold={CF.LONG_THRESHOLD}  SNR={CF.SNR_MIN}  "
        f"stop={CF.MAE_ATR_FALLBACK}×ATR  target={CF.TIER3_TARGET_ATR}×ATR", C)

    engine = LiveEngine()

    # Seed window with REST history for warmup context
    boot = await asyncio.get_event_loop().run_in_executor(
        None, lambda: bootstrap_rest(CF.SYMBOL, 300)
    )
    for bar in boot:
        engine._window.append(bar)
    if boot:
        log(f"Window seeded with {len(boot)} REST bars — now on live stream", G)

    # Seed HTF bars (5m / 15m / 1h / 4h) from REST for immediate regime context.
    log("Fetching HTF bars (5m / 15m / 1h / 4h) for regime context...", C)
    htf_data = await asyncio.get_event_loop().run_in_executor(
        None, lambda: bootstrap_htf(CF.SYMBOL)
    )
    engine._htf_5m  = htf_data.get('5m',  [])
    engine._htf_15m = htf_data.get('15m', [])
    engine._htf_1h  = htf_data.get('1h',  [])
    engine._htf_4h  = htf_data.get('4h',  [])
    log(f"HTF seeded: "
        f"{len(engine._htf_5m)}×5m  {len(engine._htf_15m)}×15m  "
        f"{len(engine._htf_1h)}×1h  {len(engine._htf_4h)}×4h bars", G)

    # Seed OB so cavitation has real depth data from bar 1, not just after the
    # first depth WebSocket push (which could be seconds into the first bar).
    try:
        from physics import data as D
        ob = await asyncio.get_event_loop().run_in_executor(
            None, lambda: D.fetch_orderbook(CF.SYMBOL, 20)
        )
        engine._bids = ob['bids']
        engine._asks = ob['asks']
        log(f"OB seeded: {len(ob['bids'])} bid levels  {len(ob['asks'])} ask levels", G)
    except Exception as e:
        log(f"OB seed error (depth stream will populate): {e}", Y)

    loop = asyncio.get_event_loop()
    for sig in (signal.SIGINT, signal.SIGTERM):
        loop.add_signal_handler(sig, engine.stop)

    await engine.run()


if __name__ == '__main__':
    asyncio.run(_main())
